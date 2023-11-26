package ru.mipt1c.homework.task2;

import org.springframework.stereotype.Service;
import ru.mipt1c.homework.task2.Dao.HasId;
import ru.mipt1c.homework.task2.Dao.MapEntry;
import ru.mipt1c.homework.task2.Exception.StorageClosedException;
import ru.mipt1c.homework.task2.Iteraror.StorageIterator;
import ru.mipt1c.homework.task2.Repository.KeyRepository;
import ru.mipt1c.homework.task2.Repository.MapRepository;
import ru.mipt1c.homework.task2.Repository.ValueRepository;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

@Service
public class DBKeyValueStorage<K, V extends HasId> implements KeyValueStorage<K, V> {

    private boolean storageOpen;

    private final MapRepository mapRepository;
    private final ValueRepository<V> valueRepository;
    private final KeyRepository<K> keyRepository;

    private final List<StorageIterator<K>> givenIterators;

    public DBKeyValueStorage(KeyRepository<K> keyRepository, ValueRepository<V> valueRepository, MapRepository mapRepository) {
        this.keyRepository = keyRepository;
        this.valueRepository = valueRepository;
        this.mapRepository = mapRepository;

        givenIterators = new ArrayList<>();
        storageOpen = false;
    }

    @Override
    public V read(K key) {
        checkStorageState();

        return mapRepository.findById(key.hashCode())
                .flatMap(me -> valueRepository.findById(me.getValId()))
                .orElse(null);
    }

    @Override
    public boolean exists(K key) {
        checkStorageState();

        return mapRepository.existsById(key.hashCode());
    }

    @Override
    public void write(K key, V value) {
        checkStorageState();

        keyRepository.save(key);
        V savedValue = valueRepository.save(value);
        mapRepository.save(new MapEntry(key.toString(), savedValue.getId()));
    }

    @Override
    public void delete(K key) {
        checkStorageState();

        mapRepository.findById(key.hashCode())
                .ifPresent(me -> valueRepository.deleteById(me.getValId()));
        keyRepository.delete(key);
    }

    @Override
    public Iterator<K> readKeys() {
        return keyRepository.findAll().iterator();
    }

    @Override
    public int size() {
        return (int) keyRepository.count();
    }

    @Override
    public void flush() {
        keyRepository.flush();
        valueRepository.flush();
        mapRepository.flush();
    }

    @Override
    public void close() throws IOException {
        storageOpen = true;
        flush();
        invalidateIterators();
    }

    private void checkStorageState() {
        if (storageOpen) {
            throw new StorageClosedException("Storage is closed.");
        }
    }

    private void invalidateIterators() {
        givenIterators.forEach(StorageIterator::invalidate);
    }
}
