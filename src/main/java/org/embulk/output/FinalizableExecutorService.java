package org.embulk.output;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class FinalizableExecutorService
{
    public static class NotCloseable
            implements Closeable
    {
        @Override
        public void close()
                throws IOException {
            // ignore
        }
    }

    protected ExecutorService threads;
    protected Queue<RunningTask> runningTasks;

    public FinalizableExecutorService() {
        this.threads = Executors.newCachedThreadPool();
        this.runningTasks = new LinkedList<>();
    }

    private static class RunningTask {
        private Future<Void> future;
        private Closeable finalizer;

        RunningTask(Future<Void> future, Closeable finalizer) {
            this.future = future;
            this.finalizer = finalizer;
        }

        public void join() throws IOException {
            try {
                future.get();
            } catch (InterruptedException ex) {
                throw new IOException(ex);
            } catch (ExecutionException ex) {
                throw new IOException(ex.getCause());
            }
            finalizer.close();
        }

        public void abort() throws IOException {
            finalizer.close();
        }
    }

    public void submit(Callable<Void> task, Closeable finalizer) {
        Future<Void> future = threads.submit(task);
        runningTasks.add(new RunningTask(future, finalizer));
    }

    public void joinPartial(long upto) throws IOException {
        while(runningTasks.size() > upto) {
            runningTasks.peek().join();
            runningTasks.remove();
        }
    }

    public void joinAll() throws IOException {
        joinPartial(0);
    }

    public void shutdown() throws IOException {
        try {
            joinAll();
        } finally {
            threads.shutdown();
            for(RunningTask task : runningTasks) {
                task.abort();
            }
        }
    }
}
