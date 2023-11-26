package ru.mipt1c.homework.task2.Dao;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Column;

@Entity(name = "map_entry")
@Getter
@Setter
@Accessors(chain = true)
@AllArgsConstructor
public class MapEntry {
    @Id
    private String keyString;
    @Column
    private Long valId;
}
