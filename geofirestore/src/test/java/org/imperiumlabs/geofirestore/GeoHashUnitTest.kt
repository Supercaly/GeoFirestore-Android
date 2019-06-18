package org.imperiumlabs.geofirestore

import com.google.common.truth.Truth.assertThat
import org.imperiumlabs.geofirestore.core.GeoHash
import org.imperiumlabs.geofirestore.util.Base32Utils
import org.junit.Test

class GeoHashUnitTest {

    @Test
    fun `is geo hash valid`() {
        //Test if the length is correct
        assertThat(GeoHash(0.0, 0.0, 12).geoHashString).hasLength(12)

        //Test various geo hashes
        assertThat(GeoHash(0.0, 0.0).geoHashString).containsMatch("7zzzzzzzzz")
        assertThat(GeoHash(90.0, 0.0).geoHashString).containsMatch("gzzzzzzzzz")
        assertThat(GeoHash(-90.0, 0.0).geoHashString).containsMatch("5bpbpbpbpb")
        assertThat(GeoHash(0.0, 180.0).geoHashString).containsMatch("rzzzzzzzzz")
        assertThat(GeoHash(0.0, -180.0).geoHashString).containsMatch("2pbpbpbpbp")
        assertThat(GeoHash(90.0, 180.0).geoHashString).containsMatch("zzzzzzzzzz")
        assertThat(GeoHash(-90.0, 180.0).geoHashString).containsMatch("pbpbpbpbpb")
        assertThat(GeoHash(90.0, -180.0).geoHashString).containsMatch("bpbpbpbpbp")
        assertThat(GeoHash(-90.0, -180.0).geoHashString).containsMatch("0000000000")

        //Test the string constructor
        assertThat(GeoHash("5bpbpbpbpbpb").geoHashString).containsMatch("5bpbpbpbpbpb")

        //Test the equals
        val geo1 = GeoHash(0.0, 0.0)
        val geo2 = GeoHash(45.0, 128.5)
        assertThat(geo1 == geo2).isFalse()

        println("GeoHash tested successfully!!!")
    }

    @Test
    fun `is string a valid Base32`() {
        //Standard string of different sizes
        assertThat(Base32Utils.isValidBase32String("7zzzzzzz")).isTrue()
        assertThat(Base32Utils.isValidBase32String("7")).isTrue()
        //Empty string
        assertThat(Base32Utils.isValidBase32String("")).isFalse()
        //Strings with errors
        assertThat(Base32Utils.isValidBase32String("7zzzzzzzcjif sic")).isFalse()
        assertThat(Base32Utils.isValidBase32String("#à")).isFalse()

        println("Base32 tested successfully!!!")
    }
}