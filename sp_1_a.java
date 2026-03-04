import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import java.io.Reader;

public final class MyBatisSessionFactory {

    private static final SqlSessionFactory FACTORY = build();

    private static SqlSessionFactory build() {
        try (Reader r = Resources.getResourceAsReader("mybatis-config.xml")) {
            return new SqlSessionFactoryBuilder().build(r);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static SqlSessionFactory get() {
        return FACTORY;
    }

    private MyBatisSessionFactory() {}
}