package com.lacoste.io.model;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.MonthDay;

public enum Signo {

//        Áries: de 21 de março a 20 de abril;
//        Touro: de 21 de abril a 20 de maio;
//        Gêmeos: de 21 de maio a 20 de junho;
//        Câncer: de 21 de junho a 22 de julho;
//        Leão: de 23 de julho a 22 de agosto;
//        Virgem: de 23 de agosto a 22 de setembro;
//        Libra: de 23 de setembro a 22 de outubro;
//        Escorpião: de 23 de outubro a 21 de novembro;
//        Sagitário: de 22 de novembro a 21 de dezembro;
//        Capricórnio: de 22 de dezembro a 20 de janeiro;
//        Aquário: de 21 de janeiro a 18 de fevereiro;
//        Peixes: de 19 de fevereiro a 20 de março;


    AQUARIO("Aquário", MonthDay.of(1, 21), MonthDay.of(2, 18)),
    PEIXES("Peixes", MonthDay.of(2, 19), MonthDay.of(3, 20)),
    ARIES("Áries", MonthDay.of(3, 21), MonthDay.of(4, 20)),
    TOURO("Touro", MonthDay.of(4, 21), MonthDay.of(5, 20)),
    GEMEOS("Gêmeos", MonthDay.of(5, 21), MonthDay.of(6, 20)),
    CANCER("Câncer", MonthDay.of(6, 21), MonthDay.of(7, 22)),
    LEAO("Leão", MonthDay.of(7, 23), MonthDay.of(8, 22)),
    VIRGEM("Virgem", MonthDay.of(8, 23), MonthDay.of(9, 22)),
    LIBRA("Libra", MonthDay.of(9, 23), MonthDay.of(10, 22)),
    ESCORPIAO("Escorpião", MonthDay.of(10, 23), MonthDay.of(11, 21)),
    SAGITARIO("Sagitário", MonthDay.of(11, 22), MonthDay.of(12, 21)),
    CAPRICORNIO("Capricórnio", MonthDay.of(12, 22), MonthDay.of(1, 20));

    final String label;
    final MonthDay startDate;
    final MonthDay endDate;

    Signo(String label, MonthDay startDate, MonthDay endDate) {
        this.label = label;
        this.startDate = startDate;
        this.endDate = endDate;
    }

    public String getLabel() {
        return this.label;
    }

    public MonthDay getStartDate() {
        return this.startDate;
    }

    public MonthDay getEndDate() {
        return this.endDate;
    }

    public static Signo getSigno(LocalDateTime localDateTime) {
        return getSigno(localDateTime.toLocalDate());
    }

    public static Signo getSigno(LocalDate localDate) {
        return doGetSigno(localDate);
    }

    private static Signo doGetSigno(LocalDate birthDate) {
        for(Signo signo : Signo.values()) {
            if(isBetween(birthDate, signo.getStartDate(), signo.getEndDate()))
                return signo;
        }

        throw new RuntimeException("Signo não encontrado!!");
    }

    private static boolean isBetween(LocalDate birthDate, MonthDay startDate, MonthDay endDate) {
        LocalDate localStartDate = LocalDate.of(birthDate.getYear(), startDate.getMonth(), startDate.getDayOfMonth());
        LocalDate localEndDate = LocalDate.of(birthDate.getYear(), endDate.getMonth(), endDate.getDayOfMonth());

        if(startDate.getMonth().equals(Month.DECEMBER))
            localStartDate = localStartDate.minusYears(1);

        return !(birthDate.isBefore(localStartDate) || birthDate.isAfter(localEndDate));
    }

    @Override
    public String toString() {
        return this.getLabel();
    }
}

//        Áries: de 21 de março a 20 de abril;
//        Touro: de 21 de abril a 20 de maio;
//        Gêmeos: de 21 de maio a 20 de junho;
//        Câncer: de 21 de junho a 22 de julho;
//        Leão: de 23 de julho a 22 de agosto;
//        Virgem: de 23 de agosto a 22 de setembro;
//        Libra: de 23 de setembro a 22 de outubro;
//        Escorpião: de 23 de outubro a 21 de novembro;
//        Sagitário: de 22 de novembro a 21 de dezembro;
//        Capricórnio: de 22 de dezembro a 20 de janeiro;
//        Aquário: de 21 de janeiro a 18 de fevereiro;
//        Peixes: de 19 de fevereiro a 20 de março;
