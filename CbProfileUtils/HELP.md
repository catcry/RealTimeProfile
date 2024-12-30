# Getting Started
### Guides

# What this application do and how?
    this application created to update couchbase profiles based on specific data file.
    It read data from provided file, use its 'MSISDN' to find related profile from PostgresSQL db.
    Then it update the couchbase document which has same id as prfile id of found record from psql db.
    # notice: it is nececury to header file contain 'MSISDN' header

# Run application command: 
1) run application with only one argument for spring config file location: 
    in this form you have to define 'bi.file.path' and 'header.file.path' in application.properties file.
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

# BI information at application properties:
bi.data.seperator=|
bi.header.seperator=|
bi.file.path=/home/snow/BI_Sample/xac
header.file.path=/home/snow/BI_Sample/BiHeaders

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