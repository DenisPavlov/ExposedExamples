plugins {
    kotlin("jvm") version "1.5.0"
}

group = "org.dpavlov"
version = "0.0.1"

repositories {
    mavenCentral()
}

dependencies {
    val exposedVersion = "0.31.1"
    val kotestVersion = "4.6.0"

    implementation("org.jetbrains.exposed:exposed-core:$exposedVersion")
    implementation("org.jetbrains.exposed:exposed-dao:$exposedVersion")
    implementation("org.jetbrains.exposed:exposed-jdbc:$exposedVersion")
    implementation("org.jetbrains.exposed:exposed-jdbc:$exposedVersion")

    implementation("ch.qos.logback:logback-classic:1.2.3")
    implementation("com.h2database:h2:1.4.197")

    testImplementation("io.kotest:kotest-runner-junit5:$kotestVersion")
    testImplementation("io.kotest:kotest-assertions-core:$kotestVersion")
}
