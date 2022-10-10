package com.lacoste.io.runners;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.MonthDay;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class Lambda {

    private Lambda() {}

    public static void run() {
        // 1 - Consumer para data formatada (dd/MM/yyyy HH:mm:ss)
        consumerParaDataFormatada();

        // 2 - Predicate para informar se a pessoa é da geração Z (nasceu entre 1995 e 2010)
        predicateParaInformarSeGeracaoZ();

        // 3 - Supplier para informar os signos com as datas
        supplierParaInformarSignosComAsDatas();

        // 4 - Ordernar datas de nascimento com o comparator
        ordernarDatasDeNascimentoComComparator();
    }

    // 1
    private static void consumerParaDataFormatada() {
        Consumer<LocalDateTime> formatPrintDateAnonymousClass = new Consumer<LocalDateTime>() {
            @Override
            public void accept(LocalDateTime localDateTime) {
                System.out.println(
                        localDateTime.format(
                                DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss")
                        )
                );
            }
        };


        Consumer<LocalDateTime> formatPrintDateLambda = localDateTime -> System.out.println(
                localDateTime.format(
                        DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss")
                )
        );

        System.out.println("\n#############################\n");

        System.out.println("Data formatada - Convencional: ");
        formatPrintDate(LocalDateTime.now());

        System.out.println("Data formatada - Classe Anônima: ");
        formatPrintDateLambda.accept(LocalDateTime.now());

        System.out.println("Data formatada - Lambda: ");
        formatPrintDateAnonymousClass.accept(LocalDateTime.now());
    }

    // 2
    private static void predicateParaInformarSeGeracaoZ() {
        Predicate<LocalDate> isGenerationZAnonymousClass = new Predicate<LocalDate>() {
            @Override
            public boolean test(LocalDate birthDate) {
                return !(birthDate.isAfter(LocalDate.of(2010, 1, 1)) ||
                        birthDate.isBefore(LocalDate.of(1995, 1, 1)));
            }
        };

        Predicate<LocalDate> isGenerationZLambda = birthDate ->
                birthDate.isBefore(LocalDate.of(2010, 1, 1)) ||
                        birthDate.isBefore(LocalDate.of(1995, 1, 1));

        System.out.println("\n#############################\n");

        System.out.println("Geração Z - Convencional: "
                + isGenerationZ(LocalDate.of(2001, 6, 22)));

        System.out.println("Geração Z - Classe Anônima: "
                + isGenerationZAnonymousClass.test(LocalDate.of(2001, 6, 22)));

        System.out.println("Geração Z - Lambda: "
                + isGenerationZLambda.test(LocalDate.of(2001, 6, 22)));
    }

    // 3
    private static void supplierParaInformarSignosComAsDatas() {
        Supplier<Map<String, List<MonthDay>>> signsAnonymous = new Supplier<Map<String, List<MonthDay>>>() {
            @Override
            public Map<String, List<MonthDay>> get() {
                var map = new HashMap<String, List<MonthDay>>();
                map.put("ARIES", List.of(MonthDay.of(3, 21), MonthDay.of(4, 20)));
                map.put("TOURO", List.of(MonthDay.of(4, 21), MonthDay.of(5, 20)));
                map.put("GEMEOS", List.of(MonthDay.of(5, 21), MonthDay.of(6, 20)));
                map.put("CANCER", List.of(MonthDay.of(6, 21), MonthDay.of(7, 22)));
                map.put("LEAO", List.of(MonthDay.of(7, 23), MonthDay.of(8, 22)));
                map.put("VIRGEM", List.of(MonthDay.of(8, 23), MonthDay.of(9, 22)));
                map.put("LIBRA", List.of(MonthDay.of(9, 23), MonthDay.of(10, 22)));
                map.put("ESCORPIAO", List.of(MonthDay.of(10, 23), MonthDay.of(11, 21)));
                map.put("SAGITÁRIO", List.of(MonthDay.of(11, 22), MonthDay.of(12, 21)));
                map.put("CAPRICÓRNIO", List.of(MonthDay.of(12, 22), MonthDay.of(1, 20)));
                map.put("AQUÁRIO", List.of(MonthDay.of(1, 21), MonthDay.of(2, 18)));
                map.put("PEIXES", List.of(MonthDay.of(2, 19), MonthDay.of(3, 20)));
                return map;
            }
        };

        Supplier<Map<String, List<MonthDay>>> signsLambda = () -> {
            var map = new HashMap<String, List<MonthDay>>();
            map.put("ARIES", List.of(MonthDay.of(3, 21), MonthDay.of(4, 20)));
            map.put("TOURO", List.of(MonthDay.of(4, 21), MonthDay.of(5, 20)));
            map.put("GEMEOS", List.of(MonthDay.of(5, 21), MonthDay.of(6, 20)));
            map.put("CANCER", List.of(MonthDay.of(6, 21), MonthDay.of(7, 22)));
            map.put("LEAO", List.of(MonthDay.of(7, 23), MonthDay.of(8, 22)));
            map.put("VIRGEM", List.of(MonthDay.of(8, 23), MonthDay.of(9, 22)));
            map.put("LIBRA", List.of(MonthDay.of(9, 23), MonthDay.of(10, 22)));
            map.put("ESCORPIAO", List.of(MonthDay.of(10, 23), MonthDay.of(11, 21)));
            map.put("SAGITÁRIO", List.of(MonthDay.of(11, 22), MonthDay.of(12, 21)));
            map.put("CAPRICÓRNIO", List.of(MonthDay.of(12, 22), MonthDay.of(1, 20)));
            map.put("AQUÁRIO", List.of(MonthDay.of(1, 21), MonthDay.of(2, 18)));
            map.put("PEIXES", List.of(MonthDay.of(2, 19), MonthDay.of(3, 20)));
            return map;
        };

        System.out.println("\n#############################\n");

        System.out.println("Signos - Convencional: ");
        System.out.println(periodSolarSigns());

        System.out.println("Signos  - Classe Anônima: ");
        System.out.println(signsAnonymous.get());

        System.out.println("Signos - Lambda: ");
        System.out.println(signsLambda.get());
    }

    // 4
    private static void ordernarDatasDeNascimentoComComparator() {
        LocalDate marco = LocalDate.of(2002, 7, 1);
        LocalDate rodrigo = LocalDate.of(2001, 6, 22);
        LocalDate guTavares = LocalDate.of(1998, 1, 28);
        LocalDate andressa = LocalDate.of(1995, 5, 25);
        LocalDate guRoberto = LocalDate.of(1996, 7, 16);
        List<LocalDate> sampleList = List.of(marco, rodrigo, guTavares, andressa, guRoberto);

        // Convencional
        List<LocalDate> list1 = new ArrayList<>(sampleList);

        // Classe anonima
        List<LocalDate> list2 = new ArrayList<>(sampleList);
        Comparator<LocalDate> anonymousSortLocalDate = new Comparator<LocalDate>() {
            @Override
            public int compare(LocalDate ld1, LocalDate ld2) {
                if (ld1.isBefore(ld2))
                    return -1;

                if (ld1.isAfter(ld2))
                    return 1;

                return 0;
            }
        };

        // Lambda
        List<LocalDate> list3 = new ArrayList<>(sampleList);
        Comparator<LocalDate> sortLocalDate = (ld1, ld2) -> {
            if (ld1.isBefore(ld2))
                return -1;

            if (ld1.isAfter(ld2))
                return 1;

            return 0;
        };

        System.out.println("\n#############################\n");

        System.out.print("Lista ordenada - Convencional: ");
        sortLocalDate(list1);

        list2.sort(anonymousSortLocalDate);
        System.out.println("Lista ordenada - Classe Anônima: " + list2);

        list3.sort(sortLocalDate);
        System.out.println("Lista ordenada - Lambda: " + list3);
    }

    private static void formatPrintDate(LocalDateTime date) {
        System.out.println(
                date.format(
                        DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss")
                )
        );
    }

    private static boolean isGenerationZ(LocalDate birthDate) {
        return !(birthDate.isAfter(LocalDate.of(2010, 1, 1)) ||
                birthDate.isBefore(LocalDate.of(1995, 1, 1)));
    }

    private static Map<String, List<MonthDay>> periodSolarSigns() {
        var map = new HashMap<String, List<MonthDay>>();
        map.put("ARIES", List.of(MonthDay.of(3, 21), MonthDay.of(4, 20)));
        map.put("TOURO", List.of(MonthDay.of(4, 21), MonthDay.of(5, 20)));
        map.put("GEMEOS", List.of(MonthDay.of(5, 21), MonthDay.of(6, 20)));
        map.put("CANCER", List.of(MonthDay.of(6, 21), MonthDay.of(7, 22)));
        map.put("LEAO", List.of(MonthDay.of(7, 23), MonthDay.of(8, 22)));
        map.put("VIRGEM", List.of(MonthDay.of(8, 23), MonthDay.of(9, 22)));
        map.put("LIBRA", List.of(MonthDay.of(9, 23), MonthDay.of(10, 22)));
        map.put("ESCORPIAO", List.of(MonthDay.of(10, 23), MonthDay.of(11, 21)));
        map.put("SAGITÁRIO", List.of(MonthDay.of(11, 22), MonthDay.of(12, 21)));
        map.put("CAPRICÓRNIO", List.of(MonthDay.of(12, 22), MonthDay.of(1, 20)));
        map.put("AQUÁRIO", List.of(MonthDay.of(1, 21), MonthDay.of(2, 18)));
        map.put("PEIXES", List.of(MonthDay.of(2, 19), MonthDay.of(3, 20)));
        return map;
    }

    private static void sortLocalDate(List<LocalDate> list) {
        list.sort(LocalDate::compareTo);
        System.out.println(list);
    }

}
