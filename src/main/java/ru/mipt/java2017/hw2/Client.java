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
import static java.lang.Math.sqrt;
import static java.lang.System.exit;
import static java.lang.System.setSecurityManager;

public class Client {
  private static final Logger logger = LoggerFactory.getLogger("Client");
  private Semaphore counted = new Semaphore(0);
  private int requestsWithoutResponseAmount = 0;
  private ArrayList<Integer> unavalaibleServers = new ArrayList<>();
  // один отвалившийся сервер = один дополнительный запрос к оставшимся
  // sum[unavalaibleServers.size()] = результат последнего запроса
  private PrimesSumCounterGrpc.PrimesSumCounterStub asyncStub;
  private ArrayList<Server> servers;

  private synchronized void addRequest() {
    ++requestsWithoutResponseAmount;
    logger.info(Integer.toString(requestsWithoutResponseAmount));
  }

  private synchronized void deleteRequest() {
    --requestsWithoutResponseAmount;
    logger.info(Integer.toString(requestsWithoutResponseAmount));
    if (requestsWithoutResponseAmount == 0) {
      counted.release(1);
    }
  }

  private class Server {
    ManagedChannel channel;
    ArrayList< ArrayList<Request.Range> > rangesLists = new ArrayList<>();
    ArrayList<Long> sums = new ArrayList<>();// sums.get(i) - результат iого запроса (сумма или -1)
    MyStreamObServer responseObServer = new MyStreamObServer();
  }

  private class MyStreamObServer implements StreamObserver<Response> {
    private boolean errorHappened = false;

    public boolean hasErrorHappened() {
      return errorHappened;
    }

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
      //Status status = Status.fromThrowable(t);
      logger.error("request failed, exception: {}", t.getMessage());
      errorHappened = true;
      redistribute();
      //finishLatch.countDown();
    }
    @Override
    public void onCompleted() {
      logger.info("interaction is finished");
      //finishLatch.countDown();
    }
  }

  private synchronized void addServerToUnavailableOnes(int serverNum) {
    if (servers.size() == unavalaibleServers.size() + 1) {
      logger.info("all servers are down");
      exit(0);
    } else {
      unavalaibleServers.add(serverNum);
    }
  }

  public static void main(String[] args) throws Exception {
    logger.info("lets go");
    if (args.length % 2 != 0 || args.length <= 2) {
      logger.info("wrong amount({}) of command line arguments: ", args.length);
      exit(1);
    }
    Client client = new Client();
    try {
      client.countSumOfPrimes(args);
    } catch (Exception e) {
      System.out.println("uncatched");
      logger.error("exception: {}", e.getMessage());
    } finally {
      client.shutdown();
    }
  }

  private void countSumOfPrimes(String[] args) {
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
    try {
      counted.acquire(1);
      long result = 0;
      for (Server s : servers) {
        for (Long sum : s.sums) {
          if (sum == -1) {
            break;
          }
          long actualSum = PrimesSum(s.rangesLists.get(s.sums.indexOf(sum)));
          if (actualSum != sum) {
            logger.info("sum for range {} = {}, got {}",
              inLogger(s.rangesLists.get(s.sums.indexOf(sum))),
              actualSum,
              sum);
          }
          result += sum;
        }
      }
      System.out.println(result);
    } catch (InterruptedException e) {
      logger.error("semaphore acquire fail in the end of countSumOfPrimes, exception: {}", e.getMessage());
    }
  }

  public Long PrimesSum(ArrayList<Request.Range> ranges) {
    long sum = 0;
    for (Request.Range range : ranges) {
      for (long number = range.getLeftBorder(); number <= range.getRightBorder(); ++number) {
        if (isPrime(number)) {
          sum += number;
        }
      }
    }
    return sum;
  }

  private boolean isPrime(long number) {
    for (long divider = 2; divider <= sqrt(number); ++divider) {
      if (number % divider == 0) {
        return false;
      }
    }
    return true;
  }

  private String inLogger(ArrayList<Request.Range> ranges) {
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

  public void countPrimesSumInSubRange(Server s) {
    logger.info("primessum {} ", Integer.toString(servers.indexOf(s)));
    logger.info(
      "performing request {} to server  {}, ranges: {}",
      unavalaibleServers.size(),
      servers.indexOf(s),
      inLogger(s.rangesLists.get(unavalaibleServers.size())));
    Request.Builder requestBuilder = Request.newBuilder();
    requestBuilder.setRequestNum(unavalaibleServers.size()).setServerNum(servers.indexOf(s));
    for (Request.Range range : s.rangesLists.get(unavalaibleServers.size())) {
      Request.Range proto_range = Request.Range.newBuilder()
                                               .setLeftBorder(range.getLeftBorder())
                                               .setRightBorder(range.getRightBorder())
                                               .build();
      requestBuilder.addRanges(proto_range);
    }
    Request request = requestBuilder.build();
    asyncStub = PrimesSumCounterGrpc.newStub(s.channel);
    s.sums.add(Long.valueOf(-1));
    try {
      asyncStub.countPrimesSum(request, s.responseObServer);
      addRequest();
    } catch (StatusRuntimeException e) {
      logger.error("request {} to server {} failed for range: {} , exception: {}",
        request.getRequestNum(),
        request.getServerNum(),
        inLogger(request.getRangesList()),
        e.getStatus());
    }
  }

  private synchronized void redistribute() {
    // находим отвалившийся сервер и перераспределяем необработанные им отрезки между остальными
    ArrayList<Request.Range> rangesToRedistribute = new ArrayList<>();
    for (int serverNum = 0; serverNum < servers.size(); ++serverNum) {
      Server s = servers.get(serverNum);
      if (!unavalaibleServers.contains(serverNum) && s.responseObServer.hasErrorHappened()) {
        for (int requestNum = 0; requestNum < s.sums.size(); ++requestNum) {
          if (s.sums.get(requestNum) == -1) {
            rangesToRedistribute.addAll(s.rangesLists.get(requestNum));
          }
        }
        logger.info("+ 1 unavailable server");
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
      deleteRequest();//отвалившийся
    }
  }
  // -3
  // -3 + 1 - 2 = -4
  // -4 + 1 = -4
  // -4 -1 + 1 = -4

  private void redistributeRanges(ArrayList<Request.Range> rangesToRedistribute) {
    long totalLength = 0;
    for (Request.Range range : rangesToRedistribute) {
      totalLength += range.getRightBorder() - range.getLeftBorder() + 1;
    }
    long serversAmount = servers.size() - unavalaibleServers.size();
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
           if (rangeNum < rangesToRedistribute.size() - 1) {
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

  public void shutdown() throws InterruptedException {
    for (Server s : servers) {
      s.channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }
  }
}