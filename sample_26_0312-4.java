public interface DtoAssembler<T> {
    T assemble(CsvRecord record);
}