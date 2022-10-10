package com.lacoste.io.mapper;

import com.lacoste.io.model.Pessoa;

import java.time.LocalDateTime;

public class PessoaMapper {

    private PessoaMapper() {}

    public static Pessoa fileStringToPessoa(String fileString) {
        String[] pessoaData = fileString.split(",");
        return new Pessoa(pessoaData[0], pessoaData[1], LocalDateTime.parse(pessoaData[2]));
    }

}
