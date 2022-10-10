package com.lacoste.io.model;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;

// baseado na seguinte tabela: https://i.pinimg.com/736x/de/ed/bd/deedbdc324729e0f00d94cc50e90a2b2--tarot.jpg
public class Ascendente {

    private static final Map<Integer, Integer> ASCENDANT_HOUR_RANGE_MAP = new HashMap<>();

    static {
        /*
        O loop é para preencher o map com a ordem dos signos de seu ascendente, onde na prática,
        temos que uma pessoa que nasce entre 6:00 e 7:59 tem o ascendente de seu próprio signo.
        Então preenchemos o map com base nessa informação, para poder usá-lo de apoio posterior-
        mente.
         */
        for(int i = 0, j = 6; i < 12; i++, j++) {
            if(j == 24)
                j = 0;

            ASCENDANT_HOUR_RANGE_MAP.put(j, i);
            ASCENDANT_HOUR_RANGE_MAP.put(++j, i);
        }
    }

    private Ascendente() {}

    public static Signo getAscendente(LocalDateTime localDateTime) {
        Signo signo = Signo.getSigno(localDateTime.toLocalDate());
        return getAscendente(localDateTime.toLocalTime(), signo);
    }

    public static Signo getAscendente(LocalTime localTime, Signo signo) {
        return doGetAscendente(localTime, signo);
    }

    /*
    Baseado na tabela citada, fazemos um loop em todos os signos, começando do próprio
    signo às 6 horas até às 8 horas, daí em diante, vem o próximo signo com o seu inter-
    valo de 2 horas.

    Exemplo: O signo de Aries, das 6 às 7:59 tem o ascendente em Áries, das 8 às 9:59 em
    Touro, que é o próximo signo, em seguida Gêmeos, Câncer e assim por diante.

    Sabendo disso, para descobrirmos corretamente o ascendente, precisamos saber sua posi-
    ção inicial.

    Usando Leão como exemplo, Leão está na quinta posição (índice 4), e então usando a hora
    do nascimento, podemos saber em quantos índices iremos pular. Então se a pessoa é do
    signo de Leão e nasceu entre 2 horas e 3:59, teremos que percorrer mais 10 signos, o
    que corresponderia ao ascendente de Gêmeos.

    Este é o cálculo realizado abaixo, e entendendo que se o 'ascendantIndex' for maior que
    12 (numero total de signos), significa que já percorremos a lista mais de uma vez, então
    precisamos subtrair a quantia total para termos o valor correto.
     */
    private static Signo doGetAscendente(LocalTime localTime, Signo signo) {
        int hour = localTime.getHour();
        int relativeAscendantSignNumber = ASCENDANT_HOUR_RANGE_MAP.get(hour);
        int ascendantIndex = signo.ordinal() + relativeAscendantSignNumber;

        if(ascendantIndex > Signo.values().length)
            ascendantIndex -= Signo.values().length;

        return Signo.values()[ascendantIndex];
    }

}
