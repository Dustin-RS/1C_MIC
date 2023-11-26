package ru.mipt1c.homework.task2.Exception;

public class StorageClosedException extends RuntimeException {
    public StorageClosedException(String message) {
        super("Storage is closed. " + message);
    }
}
