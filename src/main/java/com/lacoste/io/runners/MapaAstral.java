package com.lacoste.io.runners;

import com.lacoste.io.model.Ascendente;
import com.lacoste.io.model.Pessoa;
import com.lacoste.io.model.Signo;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

public class MapaAstral {

    private MapaAstral() {}

    public static List<String> getMapaAstralInformation(Pessoa pessoa) {
        List<String> results = new LinkedList<>();
        LocalDateTime dataNascimento = pessoa.getData();
        String cidade = pessoa.getCidade();

        results.add(getBirthDateInfo(dataNascimento));
        results.add(getZoneIdByCityName(cidade).toString());
        results.add(getZodiacInfo(dataNascimento));
        results.add(getLunarSignInfo(dataNascimento, cidade));

        return results;
    }

    private static String getBirthDateInfo(LocalDateTime birthDate) {
        LocalDateTime now = LocalDateTime.now();
        Period birthPeriod = Period.between(birthDate.toLocalDate(), now.toLocalDate());

        StringBuilder builder = new StringBuilder();
        builder.append(String.format("Idade: %d anos, %d meses e %d dias%n", birthPeriod.getYears(), birthPeriod.getMonths(), birthPeriod.getDays()));
        builder.append(String.format("Data de nascimento: %s%n", birthDate.format(DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm"))));
        builder.append(String.format("%d foi bissexto? %s%n", birthDate.getYear(), Year.isLeap(birthDate.getYear())));

        return builder.toString();
    }

    private static ZoneId getZoneIdByCityName(String cityName) {
        Optional<ZoneId> zoneId = Optional.empty();
        for (String zone : ZoneId.getAvailableZoneIds()) {
            if (zone.equals(cityName))
                zoneId = Optional.of(ZoneId.of(zone));
        }

        return zoneId.orElseGet(() -> {
            System.out.println("A ZoneId informada não foi encontrada, retornando ZoneId default (America/Sao_Paulo)");
            return ZoneId.of("America/Sao_Paulo");
        });
    }

    private static String getZodiacInfo(LocalDateTime birthDate) {
        StringBuilder builder = new StringBuilder();
        builder.append(String.format("Signo: %s%n", Signo.getSigno(birthDate)));
        builder.append(String.format("Ascendente: %s%n", Ascendente.getAscendente(birthDate)));

        return builder.toString();
    }

    private static String getLunarSignInfo(LocalDateTime birthDate, String zoneIdString) {
        String lunarSign = "Dinossauro";

        ZoneId zoneId = ZoneId.of(zoneIdString);
        if (birthDate.toLocalTime().isAfter(LocalTime.NOON)) {
            if (zoneId.equals(ZoneId.of("America/Recife")))
                lunarSign = "Casimiro";
        } else {
            if (zoneId.equals(ZoneId.of("America/Cuiaba")))
                lunarSign = "Odin";
        }

        if (zoneId.equals(ZoneId.of("America/Sao_Paulo")))
            lunarSign = "Gandalf";

        return String.format("Signo Lunar: %s", lunarSign);
    }
}