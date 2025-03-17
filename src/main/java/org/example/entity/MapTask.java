package org.example.entity;

import lombok.Getter;

@Getter
public class MapTask extends Task {

    private final String fileName;
    private final int numReduceTasks;

    public MapTask(int id, String fileName, int numReduceTasks) {
        super(id);
        this.fileName = fileName;
        this.numReduceTasks = numReduceTasks;
    }
}