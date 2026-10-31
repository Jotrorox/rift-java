description = "Paper integration for Rift: messaging lifecycle and the BungeeCord channel"

dependencies {
    api(project(":rift-client"))
    compileOnly(libs.paper.api)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.launcher)
    testImplementation(libs.paper.api)
}
