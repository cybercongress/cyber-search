package fund.cyber.api.common

import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test

class FuncTest {

    @Test
    fun searchHashFormatTest() {

        Assertions.assertThat("0x2429135b30134862b19497ba973546ef2029b170".toSearchHashFormat())
                .isEqualTo("0x2429135b30134862b19497ba973546ef2029b170")

        Assertions.assertThat("2429135b30134862b19497ba973546ef2029b170".toSearchHashFormat())
                .isEqualTo("0x2429135b30134862b19497ba973546ef2029b170")

        Assertions.assertThat("0x2429135B30134862B19497BA973546EF2029B170".toSearchHashFormat())
                .isEqualTo("0x2429135b30134862b19497ba973546ef2029b170")

        Assertions.assertThat("2429135B30134862B19497BA973546EF2029B170".toSearchHashFormat())
                .isEqualTo("0x2429135b30134862b19497ba973546ef2029b170")

        Assertions.assertThat("0X2429135B30134862B19497BA973546EF2029B170".toSearchHashFormat())
                .isEqualTo("0x2429135b30134862b19497ba973546ef2029b170")

        Assertions.assertThat("0X2429135b30134862b19497ba973546ef2029b170".toSearchHashFormat())
                .isEqualTo("0x2429135b30134862b19497ba973546ef2029b170")
    }

}
