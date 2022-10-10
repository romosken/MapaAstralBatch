package com.lacoste.io.runners;

import com.lacoste.io.database.PessoaDatabase;
import com.lacoste.io.mapper.PessoaMapper;
import com.lacoste.io.model.Pessoa;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class FileIO {

    private FileIO() {
    }

    private static final String PROJECT_PATH = System.getProperty("user.dir");
    private static final String RESOURCES_PATH = PROJECT_PATH.concat("/src").concat("/main").concat("/resources");

    public static void run() throws IOException {
        Path grupoTxtPath = Paths.get(RESOURCES_PATH, "grupo.txt");

        atualizarBancoPessoas(grupoTxtPath); // precisa preencher o DB para a classe Stream funcionar

        gerarRelatorios();
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

    private static void gerarRelatorios() throws IOException {
        var pessoas = PessoaDatabase.findAll();

        for (Pessoa pessoa : pessoas) {
            Path filePath = Paths.get(RESOURCES_PATH, pessoa.getNome() + ".txt");

            List<String> results = getResultsPessoa(pessoa);

            if (Files.exists(filePath))
                Files.delete(filePath);

            Files.createFile(filePath);
            Files.write(filePath, results);
        }
    }

    private static List<String> getResultsPessoa(Pessoa pessoa) {
        List<String> results = new LinkedList<>();
        results.add(pessoa.toString());
        results.addAll(MapaAstral.getMapaAstralInformation(pessoa));
        return results;
    }
}
