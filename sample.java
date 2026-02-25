try (SqlSession session = sqlSessionFactory.openSession();
     Cursor<MyDto> cursor = session.getMapper(MyMapper.class).selectForCsvCursor()) {

    ReflectiveCsvWriter.forClass(MyDto.class)
            .writeUtf8(Path.of("out.csv"), cursor);
}