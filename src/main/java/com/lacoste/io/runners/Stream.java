package com.lacoste.io.runners;

import com.lacoste.io.database.PessoaDatabase;
import com.lacoste.io.model.Pessoa;
import com.lacoste.io.model.Signo;

import java.time.LocalDate;
import java.time.Period;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class Stream {

    private Stream() {}

    public static void run() {
        printMethodInfo("STREAM", 15);
        List<Pessoa> pessoas = PessoaDatabase.findAll();
        obterListaDePessoasPorSignoIdade(pessoas, Signo.CANCER, 20);
        obterListaDePessoasPorGeracao(pessoas, "Z");
        obterIdadesProximaCopaDoMundo(pessoas);
        obterPessoaMaisVelhaEMaisNova(pessoas);
        calcularMediaEIdadeTotalDasPessoas(pessoas);
    }

    //1. Obter a lista de pessoas que são do signo X e tem mais de Y anos.
    private static List<Pessoa> obterListaDePessoasPorSignoIdade(List<Pessoa> pessoas, Signo signo, Integer idadeBase) {
        printMethodInfo("obterListaDePessoasPorSignoIdade()", 10);
        var pessoasSignoIdade = pessoas.stream()
                .filter(p -> p.getSigno().equals(signo))
                .filter(p -> p.getIdade() > idadeBase)
                .collect(Collectors.toList());
        System.out.printf("Lista de pessoas de %s com mais de %d anos: %s%n", signo, idadeBase, pessoasSignoIdade);
        return pessoasSignoIdade;
    }

    //2. Obter a lista e a quantidade de pessoas que são menor e maior de idade


    //3. Obter a lista de pessoas que pertencem a geração {}
    private static List<Pessoa> obterListaDePessoasPorGeracao(List<Pessoa> pessoas, String geracao) {
        printMethodInfo("obterListaDePessoasPorGeracao()", 10);
        List<Pessoa> pessoaList = pessoas.stream()
                .filter(p -> p.getGeracao().equals(geracao))
                .collect(Collectors.toList());

        System.out.printf("Lista de pessoas da geração %s : %s%n", geracao, pessoaList);
        return pessoaList;
    }

    //4. Obter a lista de todas as pessoas e informar a idade delas na próxima copa do mundo
    private static void obterIdadesProximaCopaDoMundo(List<Pessoa> pessoas) {
        printMethodInfo("obterIdadesProximaCopaDoMundo()", 10);
        LocalDate dataProximaCopa = LocalDate.of(2026, 6, 15);


        pessoas.stream()
                .forEach(p -> {
                    Period period = Period.between(p.getData().toLocalDate(), dataProximaCopa);
                    System.out.printf("%s terá %d anos, %d meses e %d dias na proxima copa%n", p.getNome(),
                            period.getYears(), period.getMonths(), period.getDays());
                });

    }

    //5. Obter a pessoa mais velha e mais nova
    private static void obterPessoaMaisVelhaEMaisNova(List<Pessoa> pessoas) {
        printMethodInfo("obterPessoaMaisVelhaEMaisNova()", 10);
        Optional<Pessoa> maisVelha = pessoas.stream().sorted(Comparator.comparing(Pessoa::getData)).findFirst();
        Optional<Pessoa> maisNova = pessoas.stream().sorted(Comparator.comparing(Pessoa::getData).reversed()).findFirst();

        System.out.printf("Pessoa mais nova: %s%nPessoa mais velha: %s%n", maisNova.get(), maisVelha.get());
    }


    //6. Calcular a idade média e total das pessoas
    private static void calcularMediaEIdadeTotalDasPessoas(List<Pessoa> pessoas) {
        printMethodInfo("calcularMediaEIdadeTotalDasPessoas()", 10);
        var idadeMedia = pessoas.stream().mapToInt(Pessoa::getIdade).average().getAsDouble();
        var idadeTotal = pessoas.stream().mapToInt(Pessoa::getIdade).sum();
        System.out.println("Idade média das pessoas: " + idadeMedia);
        System.out.println("Idade total das pessoas: " + idadeTotal);
    }

    private static void printMethodInfo(String methodLabel, int decorationCount) {
        String decoration = "#".repeat(decorationCount);
        System.out.println();
        System.out.println(decoration.concat(" ").concat(methodLabel).concat(" ").concat(decoration));
    }

    
}




