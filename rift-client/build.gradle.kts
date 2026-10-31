description = "Client for Rift's QUIC messaging bus and control subjects"

dependencies {
    implementation(libs.kwik)
    api(libs.gson)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.launcher)
}
