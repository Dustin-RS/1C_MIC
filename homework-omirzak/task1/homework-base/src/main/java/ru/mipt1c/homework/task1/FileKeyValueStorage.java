package ru.mipt1c.homework.task1;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import java.io.*;
import java.util.*;
import lombok.Cleanup;
import lombok.Value;
import lombok.SneakyThrows;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

@Component
public class FileKeyValueStorage<K extends Comparable<K>, V> implements KeyValueStorage<K, V> {

    private final File mainDirectory;
    private final Set<Iterator<K>> activeIterators;
    private boolean isClosed;

    @Value
    private class DataItem {
        File keyFile;
        File valueFile;
        K key;
        V value;
    }
    protected Map<K, V> loadAll() {
        Map<K, V> dataset = new TreeMap<K, V>();
        for (File file : Objects.requireNonNull(mainDirectory.listFiles())) {
            processDirectory(file, (DataItem item) -> {
                dataset.put(item.key, item.value);
            });
        }
        return dataset;
    }
    @SneakyThrows
    public static void serialize(Object obj, File dest) {
        @Cleanup
        FileOutputStream fos = new FileOutputStream(dest);
        @Cleanup
        ObjectOutputStream oos = new ObjectOutputStream(fos);

        oos.writeObject(obj);
    }

    @SneakyThrows
    public static Object deserialize(File fileName) {
        @Cleanup
        FileInputStream fis = new FileInputStream(fileName);
        @Cleanup
        ObjectInputStream ois = new ObjectInputStream(fis);

        return ois.readObject();
    }

    @Autowired
    public FileKeyValueStorage(String directoryPath) {
        this.mainDirectory = new File(directoryPath);
        this.isClosed = false;
        this.activeIterators = new HashSet<>();

        if (!this.mainDirectory.exists()) {
            this.mainDirectory.mkdir();
        } else {
            try {
                loadAll();
            } catch (Exception e) {
                throw new MalformedDataException();
            }
        }
    }
    private void processDirectory(File directory, Consumer<DataItem> action) {
        for (File file : Objects.requireNonNull(directory.listFiles())) {
            String filename = file.getName();
            if (!filename.contains("key")){
                continue;
            }
            String keyName = file.getName();;
            String valueName = keyName.replace("key", "value");
            File keyFile = new File(directory, keyName);
            File valueFile = new File(directory, valueName);
            action.accept(new DataItem(keyFile, valueFile, (K) deserialize(keyFile), (V) deserialize(valueFile)));
        }
    }
    @Override
    public V read(K key) {
        if (isClosed) {
            throw new RuntimeException("Attempting to write to closed datastore");
        }
        int hash = Math.abs(key.hashCode());
        File directory_for_file = new File(this.mainDirectory, Integer.toString(hash));
        if (!(directory_for_file.exists())) {
            return null;
        }
        AtomicReference<V> result = new AtomicReference<V>();
        processDirectory(directory_for_file, (DataItem item) -> {
            if (item.key.equals(key)) {
                result.set(item.value);
            }
        });
        return result.get();
    }
    @Override
    public boolean exists(K key) {
        return read(key) != null;
    }
    @Override
    public void write(K key, V value) {
        if (isClosed) {
            throw new RuntimeException("Attempting to write to closed datastore");
        }
        if (exists(key)) {
            int hash = Math.abs(key.hashCode());
            File directory = new File(this.mainDirectory, Integer.toString(hash));
            processDirectory(directory, (DataItem item) -> {
                if (key.equals(item.key)) {
                    item.valueFile.delete();
                    serialize(value, item.valueFile);
                }
            });
            return;
        }
        for (Iterator<K> iterator : activeIterators) {
            if (iterator.hasNext()) {
                throw new ConcurrentModificationException();
            }
        }
        activeIterators.clear();

        int hash = Math.abs(key.hashCode());
        final File directory = new File(this.mainDirectory, Integer.toString(hash));

        if (!directory.exists()) {
            directory.mkdir();
        }
        Random rand = new Random();
        int id = Math.abs(rand.nextInt());
        File keyFile = new File(directory, "key_" + Long.toString(id));
        File valueFile = new File(directory, "value_" + Long.toString(id));
        while (keyFile.exists() || valueFile.exists()) {
            id = Math.abs(rand.nextInt());
            keyFile = new File(directory, "key_" + Long.toString(id));
            valueFile = new File(directory, "value_" + Long.toString(id));
        }
        serialize(key, keyFile);
        serialize(value, valueFile);
    }
    @Override
    public void delete(K key) {
        if (isClosed) {
            throw new RuntimeException("Attempting to write to closed datastore");
        }
        for (Iterator<K> iterator : activeIterators) {
            if (iterator.hasNext()) {
                throw new ConcurrentModificationException();
            }
        }
        activeIterators.clear();
        int hash = Math.abs(key.hashCode());
        File directory = new File(this.mainDirectory, Integer.toString(hash));

        processDirectory(directory, (DataItem item) -> {
            if (item.key.equals(key)) {
                item.keyFile.delete();
                item.valueFile.delete();
            }
        });
    }

    @Override
    public Iterator<K> readKeys() {
        if (isClosed) {
            throw new RuntimeException("Attempting to write to closed datastore");
        }
        Iterator<K> result = loadAll().keySet().iterator();
        activeIterators.add(result);
        return result;
    }

    @Override
    public int size() {
        if (isClosed) {
            throw new RuntimeException("Attempting to write to closed datastore");
        }
        return loadAll().size();
    }

    @Override
    public void flush() {
    }

    @Override
    public void close() throws IOException {
        this.isClosed = true;
    }
}
