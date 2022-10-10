package com.lacoste.io.database;

import com.lacoste.io.model.Pessoa;

import java.util.*;

public class PessoaDatabase {

    private PessoaDatabase() {}

    private static Long id = 0L;

    private static final Map<Long, Pessoa> PESSOAS_DB = new HashMap<>();

    public static void save(Pessoa pessoa) {
        PESSOAS_DB.put(++id, pessoa);
    }

    public static void saveAll(List<Pessoa> pessoas) {
        pessoas.forEach(PessoaDatabase::save);
    }

    public static List<Pessoa> findAll() {
        return new ArrayList<>(PESSOAS_DB.values());
    }

}
