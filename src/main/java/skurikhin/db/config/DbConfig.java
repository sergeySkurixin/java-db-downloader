package skurikhin.db.config;

import lombok.Data;

@Data
public class DbConfig {
    private String url;
    private String user;
    private String password;
}
