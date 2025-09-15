import com.ericsson.iot.ct.model.ServiceStatus;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.configuration.FluentConfiguration;
import org.postgresql.ds.PGSimpleDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.HashMap;

@Service
public class EmbeddedPG extends AbstractService {
    private static final Logger LOG = LoggerFactory.getLogger(EmbeddedPG.class);

    private EmbeddedPostgres postgres;

    @Value("${pg.port}")
    private int port;

    private DataSource dataSource;
    private Flyway flyway;

    HashMap<String, String> properties = new HashMap<>(16);

    @Override
    public void start() {
        LOG.debug("Start embedded PG.");
        try {
            Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
            dataSource = getDatabase();
            postgres = EmbeddedPostgres.builder().setPort(port).setPGStartupWait(Duration.ofSeconds(20L)).start();
            assert postgres != null;
            LOG.debug("Embedded PG starts at localhost:{}", port);
        } catch (Exception e) {
            LOG.error("Failed to start embedded PG." + e.getMessage(), e);
            this.setFinalStatus(ServiceStatus.DOWN);
        }
        if(isHealth()){
            init();
        }
    }

    private void init() {
        LOG.debug("Initialize profile_service schema.");
        FluentConfiguration fluentConfiguration = Flyway.configure().dataSource(dataSource);
        flyway = new Flyway(fluentConfiguration);
        flyway.migrate();
    }

    @Override
    public void stop() {
        if (flyway != null) {
            try {
                flyway.clean();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (postgres != null) {
            try {
                postgres.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public boolean isHealth() {
        boolean health;
        try(Connection c = dataSource.getConnection();
            Statement s = c.createStatement();
            ResultSet rs = s.executeQuery("SELECT 1");
        )
        {
            health = rs.next() && (rs.getInt(1) == 1) && !rs.next();
        } catch (Exception e) {
            health = false;
        }
        return health;
    }

    @Override
    public String getServiceName() {
        return "pg";
    }

    public DataSource getDatabase() {

        String dbName = "postgres";
        String userName = "postgres";
        final PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setServerName("localhost");
        ds.setPortNumber(port);
        ds.setDatabaseName(dbName);
        ds.setUser(userName);

        properties.forEach((propertyKey, propertyValue) -> {
            try {
                ds.setProperty(propertyKey, propertyValue);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        return ds;
    }
}
