package ru.csc.bdse.kv.db;

import org.jetbrains.annotations.NotNull;
import javax.persistence.*;

@javax.persistence.Entity
@Table(name = "Entity")
public class Entity {

    @Id @Column(name = "_key")
    private String key;

    @Column(name = "_value")
    private byte[] value;

    public Entity() {} // POJO

    public Entity(@NotNull String key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return this.key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public byte[] getValue() {
        return this.value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

}
