package org.example.entity;

import lombok.Getter;

import java.util.List;


@Getter
public class ReduceTask extends Task {

    private List<String> fileNames;

    public ReduceTask(int id, List<String> fileNames) {
        super(id);
        this.fileNames = fileNames;
    }
}