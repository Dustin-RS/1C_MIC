package ru.mipt1c.homework.task2.Iteraror;

import ru.mipt1c.homework.task2.Exception.StorageClosedException;

import java.util.Iterator;

public class StorageIterator<K> implements Iterator<K> {

    private final Iterator<K> underlyingIterator;
    private boolean isValid;

    public StorageIterator(Iterator<K> iter) {
        underlyingIterator = iter;
        isValid = true;
    }

    @Override
    public boolean hasNext() {
        if (!isValid) {
            throw new StorageClosedException("Can not iterate.");
        }

        return underlyingIterator.hasNext();
    }

    @Override
    public K next() {
        if (!isValid) {
            throw new StorageClosedException("Can not iterate.");
        }

        return underlyingIterator.next();
    }

    public void invalidate() {
        isValid = false;
    }
}
