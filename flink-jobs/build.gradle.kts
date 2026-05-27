plugins {
    java
    id("com.github.johnrengelman.shadow") version "8.1.1" apply false
}

subprojects {
    apply(plugin = "java")
    apply(plugin = "com.github.johnrengelman.shadow")

    group = "org.openphc.cce.pipeline"
    version = "1.0.0-SNAPSHOT"

    java {
        sourceCompatibility = JavaVersion.VERSION_21
        targetCompatibility = JavaVersion.VERSION_21
    }

    repositories {
        mavenCentral()
    }

    val flinkVersion = "1.19.1"
    val jacksonVersion = "2.17.0"
    val slf4jVersion = "2.0.12"

    dependencies {
        // Flink core (provided at runtime by the cluster)
        compileOnly("org.apache.flink:flink-streaming-java:$flinkVersion")
        compileOnly("org.apache.flink:flink-clients:$flinkVersion")
        compileOnly("org.apache.flink:flink-runtime-web:$flinkVersion")

        // Flink connectors (bundled in fat JAR)
        implementation("org.apache.flink:flink-connector-kafka:3.1.0-1.18")
        implementation("org.apache.flink:flink-connector-jdbc:3.1.2-1.18")

        // ClickHouse JDBC driver
        implementation("com.clickhouse:clickhouse-jdbc:0.6.0")
        implementation("org.apache.httpcomponents.client5:httpclient5:5.3.1")

        // JSON processing
        implementation("com.fasterxml.jackson.core:jackson-databind:$jacksonVersion")
        implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:$jacksonVersion")

        // Logging
        implementation("org.slf4j:slf4j-api:$slf4jVersion")
        runtimeOnly("org.apache.logging.log4j:log4j-slf4j2-impl:2.23.1")
        runtimeOnly("org.apache.logging.log4j:log4j-core:2.23.1")

        // Test
        testImplementation("org.apache.flink:flink-streaming-java:$flinkVersion")
        testImplementation("org.apache.flink:flink-test-utils:$flinkVersion")
        testImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
        testImplementation("org.assertj:assertj-core:3.25.3")
        testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    }

    tasks.test {
        useJUnitPlatform()
    }

    tasks.named<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar>("shadowJar") {
        archiveBaseName.set(project.name)
        archiveClassifier.set("all")
        mergeServiceFiles()
        manifest {
            attributes["Main-Class"] = "org.openphc.cce.pipeline.${project.name.replace("-", "").replaceFirstChar { it.uppercase() }}Job"
        }
    }
}
