package ru.mipt.java2017.hw2;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mipt.java2017.hw2.Request.Range;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.System.exit;

public class Client {
  private static final Logger logger = LoggerFactory.getLogger("Client");
  private Semaphore counted = new Semaphore(0);
  /**
   * main thread блокируется на семафоре до получения ответа по всем запросам
   */
  private int requestsWithoutResponseAmount = 0;
  private ArrayList<Integer> unavalaibleServers = new ArrayList<>();
  /**
   * один отвалившийся сервер = один дополнительный запрос к оставшимся
   * sum.get(unavalaibleServers.size()) = результат последнего запроса
   */
  private PrimesSumCounterGrpc.PrimesSumCounterStub asyncStub;
  private ArrayList<Server> servers;

  private class Server {
    ManagedChannel channel;
    ArrayList< ArrayList<Request.Range> > rangesLists = new ArrayList<>();
    /**
     * rangesLists.get(i) - список промежутков iого запроса
     */
    ArrayList<Long> sums = new ArrayList<>();
    /**
     * sums.get(i) - результат iого запроса (сумма или -1)
     */
     MyStreamObServer responseObServer = new MyStreamObServer();
  }

  public static void main(String[] args) throws Exception {
    logger.info("start");
    if (args.length % 2 != 0 || args.length <= 2) {
      logger.info("wrong amount of command line arguments");
      exit(0);
    }
    Client client = new Client();
    try {
      client.initialRequests(args);
      client.countSumOfResponses();
    } catch (Exception e) {
      logger.error("uncatched exception: {}", e.getClass());
    } finally {
      client.shutdown();
    }
  }

  private void initialRequests(String[] args) {
    long leftBorder = Long.parseLong(args[0]);
    long rightBorder = Long.parseLong(args[1]);
    int serversAmount = (args.length - 2) / 2;
    servers = new ArrayList<>(serversAmount);
    long segmentLength = max(1, (rightBorder - leftBorder + 1) / serversAmount);
    int rest = (int)(rightBorder - leftBorder + 1) % serversAmount;
    int argNum = 2;
    for (int ServerNum = 0; ServerNum < serversAmount; ++ServerNum) {
      Server s = new Server();
      s.channel = ManagedChannelBuilder.forAddress(args[argNum], Integer.parseInt(args[argNum + 1]))
                                       .usePlaintext(true)
                                       .build();
      Request.Range.Builder rangeBuilder = Request.Range.newBuilder();
      rangeBuilder.setLeftBorder(leftBorder);
      if (rest > 0) {
        rangeBuilder.setRightBorder(leftBorder + segmentLength);
        --rest;
        leftBorder += segmentLength + 1;
      } else {
        rangeBuilder.setRightBorder(leftBorder + segmentLength - 1);
        leftBorder += segmentLength;
      }
      Request.Range range = rangeBuilder.build();
      ArrayList<Request.Range> rangesList = new ArrayList<>();
      rangesList.add(range);
      s.rangesLists.add(rangesList);
      argNum += 2;
      servers.add(ServerNum, s);
    }
    for (Server s : servers) {
      countPrimesSumInSubRange(s);
    }
  }

  private void countPrimesSumInSubRange(Server s) {
    logger.info("performing request {} to server {}, ranges: {}",
      unavalaibleServers.size(),
      servers.indexOf(s),
      inLogger(s.rangesLists.get(unavalaibleServers.size())));
    Request request = Request.newBuilder()
                             .setRequestNum(unavalaibleServers.size())
                             .setServerNum(servers.indexOf(s))
                             .addAllRanges(s.rangesLists.get(unavalaibleServers.size()))
                             .build();
    asyncStub = PrimesSumCounterGrpc.newStub(s.channel);
    s.sums.add(Long.valueOf(-1));
    try {
      asyncStub.countPrimesSum(request, s.responseObServer);
      addRequest();
    } catch (StatusRuntimeException e) {
      logger.error("request {} to server {} failed for range: {} , exception status: {}",
        request.getRequestNum(),
        request.getServerNum(),
        inLogger(request.getRangesList()),
        e.getStatus());
    }
  }

  private class MyStreamObServer implements StreamObserver<Response> {
    private boolean errorHappened = false;

    @Override
    public void onNext(Response response) {
      logger.info("got reponse {} from server {}, sum = {}",
        response.getRequestNum(),
        response.getServerNum(),
        response.getSum());
      Server s = servers.get(response.getServerNum());
      s.sums.set(response.getRequestNum(), response.getSum());
      deleteRequest();
    }

    @Override
    public void onError(Throwable t) {
      logger.error("request failed, exception: {}", t.getMessage());
      errorHappened = true;
      redistribute();
    }
    @Override
    public void onCompleted() {
      logger.info("interaction is finished");
    }
  }

  /**
   * находит отвалившийся сервер и перераспределяет необработанные им запросы между остальными
   */
  private synchronized void redistribute() {
    ArrayList<Request.Range> rangesToRedistribute = new ArrayList<>();
    for (int serverNum = 0; serverNum < servers.size(); ++serverNum) {
      Server s = servers.get(serverNum);
      if (!unavalaibleServers.contains(serverNum) && s.responseObServer.errorHappened) {
        for (int requestNum = 0; requestNum < s.sums.size(); ++requestNum) {
          if (s.sums.get(requestNum) == -1) {
            rangesToRedistribute.addAll(s.rangesLists.get(requestNum));
          }
        }
        logger.info("server {} is unavailable", serverNum);
        addServerToUnavailableOnes(serverNum);
        break;
      }
    }
    if (rangesToRedistribute.size() > 0) {
      redistributeRanges(rangesToRedistribute);
      for (int serverNum = 0; serverNum < servers.size(); ++serverNum) {
        if (!unavalaibleServers.contains(serverNum)) {
          countPrimesSumInSubRange(servers.get(serverNum));
        }
      }
      deleteRequest(); // отвалившийся
    } // rangesToRedistribute.size() == 0, если сервер уже был помечен как недоступный
  }

  private void redistributeRanges(ArrayList<Request.Range> rangesToRedistribute) {
    long totalLength = 0;
    for (Request.Range range : rangesToRedistribute) {
      totalLength += range.getRightBorder() - range.getLeftBorder() + 1;
    }
    int serversAmount = servers.size() - unavalaibleServers.size();
    long segmentLength = max(1, totalLength / serversAmount);
    int rest = (int)(totalLength % serversAmount);
    int rangeNum = 0;
    Request.Range range = rangesToRedistribute.get(0);
    for (Server s : servers) {
      if (unavalaibleServers.contains(servers.indexOf(s))) {
        continue;
      }
      s.rangesLists.add(new ArrayList<Request.Range>());
      long alreadyAddedLen = 0;
      while (alreadyAddedLen < segmentLength + ((rest != 0) ? 1 : 0)) {
        long rightBorder = min(range.getRightBorder(), range.getLeftBorder() + segmentLength - alreadyAddedLen - ((rest == 0) ? 1 : 0));
        alreadyAddedLen += rightBorder - range.getLeftBorder() + 1;
        if (rightBorder == range.getRightBorder()) {
          s.rangesLists.get(unavalaibleServers.size()).add(range);
          if (rangeNum != rangesToRedistribute.size() - 1) {
            range = rangesToRedistribute.get(++rangeNum);
          }
        } else {
          Request.Range rangeToAdd = Request.Range.newBuilder()
                                                  .setLeftBorder(range.getLeftBorder())
                                                  .setRightBorder(rightBorder)
                                                  .build();
          s.rangesLists.get(unavalaibleServers.size()).add(rangeToAdd);
          range = Request.Range.newBuilder()
                               .setLeftBorder(rightBorder + 1)
                               .setRightBorder(range.getRightBorder())
                               .build();
        }
        if (rest > 0) {
          --rest;
        }
      }
    }
  }

  private void countSumOfResponses() {
    try {
      counted.acquire(1);
      long result = 0;
      for (Server s : servers) {
        for (Long sum : s.sums) {
          if (sum == -1) {
            break;
            // следующие после первого неудачного запросы, если и были сделаны, то к недоступному серверу,
            // а, значит, эти запросы были перенаправлены к другим серверам
          }
          result += sum;
        }
      }
      System.out.println(result);
    } catch (InterruptedException e) {
      logger.error("semaphore acquire fail in countSumOfResponses, exception message: {}", e.getMessage());
    }
  }

  private synchronized void addRequest() {
    ++requestsWithoutResponseAmount;
    logger.info("sent request, amount of requests without response = {}",
      Integer.toString(requestsWithoutResponseAmount));
  }

  private synchronized void deleteRequest() {
    --requestsWithoutResponseAmount;
    logger.info("got response, amount of requests without response = {}",
      Integer.toString(requestsWithoutResponseAmount));
    if (requestsWithoutResponseAmount == 0) {
      counted.release(1);
    }
  }

  private void addServerToUnavailableOnes(int serverNum) {
    if (servers.size() == unavalaibleServers.size() + 1) {
      logger.info("all servers are down");
      exit(0);
    } else {
      unavalaibleServers.add(serverNum);
    }
  }

  private void shutdown() throws InterruptedException {
    for (Server s : servers) {
      s.channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  private String inLogger(List<Range> ranges) {
    StringBuilder s = new StringBuilder();
    s.append('[');
    for (Request.Range range : ranges) {
      s.append(range.getLeftBorder());
      s.append('-');
      s.append(range.getRightBorder());
      s.append(", ");
    }
    s.append(']');
    return s.toString();
  }
}