plugins {
    id("checkstyle")
    id("java")
    id("me.champeau.jmh") version "0.7.1"
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

group = "column.store"
version = "0.0.1"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.commons:commons-csv:1.10.0")
    implementation("org.apache.commons:commons-lang3:3.14.0")
    implementation("org.apache.parquet:parquet-common:1.13.0")
    implementation("org.apache.parquet:parquet-encoding:1.13.0")
    implementation("org.apache.parquet:parquet-column:1.13.0")
    implementation("org.apache.parquet:parquet-hadoop:1.13.0")
    implementation("org.apache.hadoop:hadoop-common:3.3.6")
    implementation("org.apache.hadoop:hadoop-mapreduce:2.7.0")
    implementation("org.apache.hadoop:hadoop-mapreduce-client-core:3.3.6")
    implementation("com.globalmentor:hadoop-bare-naked-local-fs:0.1.0")

    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.assertj:assertj-core:3.11.1")
    testImplementation("org.mockito:mockito-core:5.7.0")
}

tasks.test {
    useJUnitPlatform()
}