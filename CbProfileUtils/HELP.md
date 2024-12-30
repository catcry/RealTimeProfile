# Getting Started
### Guides

The following guides illustrate how to use some features concretely:

# Run application command: 
1) run application with only one argument for spring config file location: 
    in this form you have to define 'biFilePath' and 'headerFilePath' in application.properties file.
    - command:
        java -jar current-mci-profile-utils-1.0.0.jar --spring.config.location=/path to application properties file//application.properties
    - sample:
        java -jar profile-utils-1.1.1.jar --spring.config.location=/home/snow/CurrentMCIProfileUtils/application.properties
2) run application with 3 arguments for spring config, BI data and BI header file path:
   in this form there is no need to define 'biFilePath' and 'headerFilePath' in application.properties file.
   - command:
        java -jar current-mci-profile-utils-1.0.0.jar --spring.config.location=application-properties-path/application.properties BI-data-path BI-header-path
   - sample:     
        java -jar current-mci-profile-utils-1.1.1.jar --spring.config.location=/home/snow/CurrentMCIProfileUtils/application.properties /home/snow/BI_Sample/xac /home/snow/BI_Sample/BiHeaders

# BI File paths at application properties:
biFilePath=/home/snow/BI_Sample/xac
headerFilePath=/home/snow/BI_Sample/BiHeaders

# Couchbase config at application properties: 
spring.couchbase.connection-string=couchbase://192.168.5.212:18091=manager
spring.couchbase.username=Administrator
spring.couchbase.password=Bonyan123
spring.couchbase.env.timeouts.connect=20s
spring.couchbase.env.timeouts.key-value-durable=20s

# PostgreSQL configuration at application properties:
spring.jpa.properties.hibernate.dialect=org.hibernate.dialect.PostgreSQLDialect
spring.datasource.url=jdbc:postgresql://192.168.5.212:5432/rtp
spring.datasource.username=rtp
spring.datasource.password=Bonyan123
spring.jpa.show-sql=true
spring.jpa.hibernate.ddl-auto=update