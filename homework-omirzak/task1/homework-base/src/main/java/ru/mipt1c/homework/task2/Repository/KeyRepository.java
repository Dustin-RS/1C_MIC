package ru.mipt1c.homework.task2.Repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface KeyRepository<Key> extends JpaRepository<Key, Key> {
}
