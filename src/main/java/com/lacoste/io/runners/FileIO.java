package com.lacoste.io.runners;

import com.lacoste.io.database.PessoaDatabase;
import com.lacoste.io.mapper.PessoaMapper;
import com.lacoste.io.model.Pessoa;
import lombok.SneakyThrows;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class FileIO {

    private FileIO() {
    }

    private static final Executor threadPool = Executors.newFixedThreadPool(5);
    private static final String PROJECT_PATH = System.getProperty("user.dir");
    private static final String RESOURCES_PATH = PROJECT_PATH.concat("/src").concat("/main").concat("/resources");

    public static void run() {
        Path grupoTxtPath = Paths.get(RESOURCES_PATH, "grupo.txt");

        atualizarBancoPessoas(grupoTxtPath); // precisa preencher o DB para a classe Stream funcionar

        System.out.println("\nExecutando com threads...");

        var time = System.currentTimeMillis();
        gerarRelatoriosThreads();
        System.out.printf("Tempo de execução com threads: %d ms%n", System.currentTimeMillis() - time);

        System.out.println("\nExecutando sem threads...");

        var time2 = System.currentTimeMillis();
        gerarRelatorios();
        System.out.printf("Tempo de execução sem threads: %d ms", System.currentTimeMillis() - time2);

    }

    private static void atualizarBancoPessoas(Path arquivo) {

        PessoaDatabase.saveAll(lerArquivoPessoas(arquivo));
    }

    private static List<Pessoa> lerArquivoPessoas(Path arquivo) {
        try {
            List<String> lines = Files.readAllLines(arquivo);

            return lines.stream()
                    .map(PessoaMapper::fileStringToPessoa)
                    .collect(Collectors.toList());

        } catch (IOException e) {
            throw new RuntimeException("Arquivo não encontrado!");
        }
    }

    private static List<List<String>> gerarRelatoriosThreads() {
        var futures = PessoaDatabase.findAll()
                .stream()
                .map(pessoa -> CompletableFuture.supplyAsync(
                        () -> escreverArquivosPessoas(pessoa, getResultadosPessoa(pessoa))
                        , threadPool))
                .collect(Collectors.toList());

        return futures.stream()
                .map(CompletableFuture::join)
                .collect(Collectors.toList());
    }
    private static List<List<String>> gerarRelatorios() {
        return PessoaDatabase.findAll()
                .stream()
                .map(pessoa -> escreverArquivosPessoas(pessoa, getResultadosPessoa(pessoa)))
                .collect(Collectors.toList());
    }

    @SneakyThrows
    private static List<String> escreverArquivosPessoas(Pessoa pessoa, List<String> results) {

        TimeUnit.SECONDS.sleep(1);
        Path filePath = Paths.get(RESOURCES_PATH, pessoa.getNome() + ".txt");

        if (Files.exists(filePath))
            Files.delete(filePath);

        Files.createFile(filePath);
        Files.write(filePath, results);

        return results;
    }


    @SneakyThrows
    private static List<String> getResultadosPessoa(Pessoa pessoa) {
        List<String> results = new LinkedList<>();
        results.add(pessoa.toString());
        results.addAll(MapaAstral.getMapaAstralInformation(pessoa));
        return results;
    }
}
