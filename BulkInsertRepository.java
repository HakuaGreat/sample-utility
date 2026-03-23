import java.util.List;

public interface BulkInsertRepository<T> {
    void insert(List<T> list);
}