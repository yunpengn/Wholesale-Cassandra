package edu.cs4224;

public class Utils {

    @FunctionalInterface
    public interface BiThrowingConsumer<T, U> {
        void accept(T var1, U var2) throws Exception;
    }
}
