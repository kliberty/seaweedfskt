import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import com.google.protobuf.gradle.*

plugins {
    kotlin("jvm") version "1.6.10"
    `maven-publish`

//    id("com.cognifide.common") version "1.0.37"
    id("com.google.protobuf") version "0.8.18"
}

group = "me.kliberty"
version = "1.0-SNAPSHOT"

ext["grpcVersion"] = "1.36.0" // CURRENT_GRPC_VERSION
ext["protobufVersion"] = "3.17.0"
ext["coroutinesVersion"] = "1.3.3"
ext["grpcKotlinVersion"] = "1.2.1"

repositories {
    mavenCentral()
    jcenter()
}
dependencies {
//    protobuf(project(":proto"))
//    sourceSets.getByName("main").resources.srcDir("seaweed")
    api(kotlin("stdlib"))
    api("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.6.0")

    api("io.grpc:grpc-protobuf:${rootProject.ext["grpcVersion"]}")
    api("com.google.protobuf:protobuf-java-util:${rootProject.ext["protobufVersion"]}")
    api("com.google.protobuf:protobuf-kotlin:${rootProject.ext["protobufVersion"]}")
    api("io.grpc:grpc-kotlin-stub:${rootProject.ext["grpcKotlinVersion"]}")

    implementation("org.apache.httpcomponents:httpmime:_")
    implementation("com.moandjiezana.toml:toml4j:_")
    implementation("io.grpc:grpc-netty-shaded:${rootProject.ext["grpcVersion"]}")
    implementation("org.slf4j:slf4j-simple:_")
    implementation("junit:junit:_")
    implementation("javax.annotation:javax.annotation-api:_")
}

tasks.withType<KotlinCompile>() {
    kotlinOptions.jvmTarget = "11"
}

//sourceSets.getByName("main") {
//    proto {
//        java.srcDir("src/main/...")
//    }
//}


protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:${rootProject.ext["protobufVersion"]}"
    }
    plugins {
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:${rootProject.ext["grpcVersion"]}"
        }
        id("grpckt") {
            artifact = "io.grpc:protoc-gen-grpc-kotlin:${rootProject.ext["grpcKotlinVersion"]}:jdk7@jar"
        }
    }
    generateProtoTasks {
        all().forEach {
            it.plugins {
                id("grpc")
                id("grpckt")
            }
//            it.builtins {
//                id("kotlin")
//            }
        }
    }
}

publishing {
    publications {
        register<MavenPublication>("gpr") {
            from(components["java"])
        }
    }
}