A backend-loader for Cassandra, to be used by the BL Monitrix project, based upon Apache commons-io and the LMAX RingBuffer / Disruptor pattern. 
Succinct documentation is coming soon.

# Troubleshooting

Note: on my machine, I had problems retrieving all managed dependencies. Make sure you have a settings.xml in your ~/.m2 folder like that:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<settings xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.1.0 http://maven.apache.org/xsd/settings-1.1.0.xsd" xmlns="http://maven.apache.org/SETTINGS/1.1.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <servers>
    <server>
      <username>admin</username>
      <id>central</id>
    </server>
    <server>
      <username>admin</username>
      <id>snapshots</id>
    </server>
  </servers>
  <activeProfiles>
    <activeProfile>artifactory</activeProfile>
  </activeProfiles>
  <!-- This seems to be *really* important -->
  <mirrors>
    <mirror>
      <id>central-mirror</id>
      <url>http://repo.maven.apache.org/maven2</url>
      <mirrorOf>*,!eclipselink</mirrorOf>
    </mirror>
  </mirrors>
</settings> 
```
 