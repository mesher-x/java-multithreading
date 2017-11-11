package ru.mipt.java2017.hw2;

import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import ru.mipt.java2017.hw2.Request.Range;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.sqrt;

public class Server {
  private static final Logger logger = LoggerFactory.getLogger("Server");
  private io.grpc.Server baseServer;

  public static void main(String[] args) throws IOException, InterruptedException {
    final Server server = new Server();
    server.start(Integer.parseInt(args[1]), Integer.parseInt(args[0]));
    server.blockUntilShutdown();
  }

  private void start(int port, int coresAmount) throws IOException {
    baseServer = ServerBuilder.forPort(port)
      .addService(new PrimesSumCounterImpl(coresAmount))
      .build()
      .start();
    logger.info("Server started, listening on {}", port);
    Runtime.getRuntime().addShutdownHook(
      new Thread() {
        @Override
        public void run() {
          System.err.println("*** shutting down gRPC server since JVM is shutting down");
          Server.this.stop();
          System.err.println("*** server shut down");
      }
    });
  }

  class PrimesSumCounterImpl extends PrimesSumCounterGrpc.PrimesSumCounterImplBase {
    private Semaphore semaphore = new Semaphore(0);
    ExecutorService executor;
    ArrayList< Future<Long> > futures;
    int coresAmount;

    PrimesSumCounterImpl(int coresAmount) {
      this.coresAmount = coresAmount;
      executor = Executors.newFixedThreadPool(coresAmount);
      futures = new ArrayList< Future<Long> >(coresAmount);
    }

    @Override
    public void countPrimesSum(Request request, StreamObserver<Response> responseObserver) {
      logger.info("Being a client's server number {} received request number {} with ranges: {}",
        request.getServerNum(),
        request.getRequestNum(),
        inLogger(request.getRangesList()));
      ArrayList< ArrayList<Request.Range> > rangesLists;
      rangesLists = redistributeRanges(request.getRangesList());
      for (int core = 0; core < coresAmount; ++core) {
        futures.add(core, executor.submit(new PrimesSumInSubRangeCounter(rangesLists.get(core))));
      }
      try {
        semaphore.acquire(coresAmount);
      } catch (InterruptedException e) {
        logger.error("semaphore acquire failure in the countPrimesSum, exception message: {}", e.getMessage());
        stop();
      }
      long sum = 0;
      for (int core = 0; core < coresAmount;++core) {
        Future<Long> future = futures.get(core);
        try {
          sum += future.get();
        } catch (InterruptedException | ExecutionException e) {
          logger.error("while getting result from future, exception: {}", e.getClass());
        }
      }
      logger.info("send request {} to client being his server {} for range: {}",
        request.getRequestNum(),
        request.getServerNum(),
        inLogger(request.getRangesList()));
      responseObserver.onNext(Response.newBuilder()
                                      .setRequestNum(request.getRequestNum())
                                      .setServerNum(request.getServerNum())
                                      .setSum(sum)
                                      .build());
      responseObserver.onCompleted();
      logger.info("finish interaction between client and server {} for request {} for range: {}, with sum = {}",
        request.getServerNum(),
        request.getRequestNum(),
        inLogger(request.getRangesList()),
        sum);
    }

    /**
     * распределяет промежутки между ядрами
     */
    private ArrayList<ArrayList<Request.Range>> redistributeRanges(List<Request.Range> rangesToRedistribute) {
      ArrayList<ArrayList<Request.Range>> redistributedRangesLists = new ArrayList<ArrayList<Request.Range>>();
      long totalLength = 0;
      for (Request.Range range : rangesToRedistribute) {
        totalLength += range.getRightBorder() - range.getLeftBorder() + 1;
      }
      long segmentLength = max(1, totalLength / coresAmount);
      int rest = (int) (totalLength % coresAmount);
      int rangeNum = 0;
      Request.Range range = rangesToRedistribute.get(0);
      for (int coreNum = 0; coreNum < coresAmount; ++coreNum) {
        long alreadyAddedLen = 0;
        redistributedRangesLists.add(new ArrayList<Request.Range>());
        while (alreadyAddedLen < segmentLength + ((rest != 0) ? 1 : 0)) {
          long rightBorder = min(range.getRightBorder(), range.getLeftBorder() + segmentLength - alreadyAddedLen - ((rest == 0) ? 1 : 0));
          alreadyAddedLen += rightBorder - range.getLeftBorder() + 1;
          if (rightBorder == range.getRightBorder()) {
            redistributedRangesLists.get(coreNum).add(range);
            if (rangeNum != rangesToRedistribute.size() - 1) {
              range = rangesToRedistribute.get(++rangeNum);
            }
          } else {
            Request.Range rangeToAdd = Request.Range.newBuilder()
                                                    .setLeftBorder(range.getLeftBorder())
                                                    .setRightBorder(rightBorder)
                                                    .build();
            redistributedRangesLists.get(coreNum).add(rangeToAdd);
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
      return redistributedRangesLists;
    }

    private class PrimesSumInSubRangeCounter implements Callable<Long> {
      private ArrayList<Request.Range> ranges;

      PrimesSumInSubRangeCounter(ArrayList<Request.Range> ranges) {
        this.ranges = ranges;
      }

      @Override
      public Long call() throws Exception {
        long sum = 0;
        for (Request.Range range : ranges) {
          for (long number = range.getLeftBorder(); number <= range.getRightBorder(); ++number) {
            if (isPrime(number)) {
              sum += number;
            }
          }
        }
        semaphore.release(1);
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
    }
  }

  private void stop() {
    if (baseServer != null) {
      baseServer.shutdown();
    }
  }

  private void blockUntilShutdown() throws InterruptedException {
    if (baseServer != null) {
      baseServer.awaitTermination();
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
