package com.aitts.queuetts.gateway.api.utils

import java.sql.ResultSet
import java.sql.Timestamp
import java.time.OffsetDateTime
import java.time.ZoneOffset

internal object JdbcValueConverters {
    fun offsetDateTime(rs: ResultSet, column: String): OffsetDateTime? =
        runCatching { rs.getObject(column, OffsetDateTime::class.java) }.getOrNull()
            ?: rs.getTimestamp(column)?.let(::timestampToOffsetDateTime)

    fun nullableDouble(rs: ResultSet, column: String): Double? {
        val value = rs.getDouble(column)
        return if (rs.wasNull()) null else value
    }

    fun nullableInt(rs: ResultSet, column: String): Int? {
        val value = rs.getInt(column)
        return if (rs.wasNull()) null else value
    }

    fun nullableLong(rs: ResultSet, column: String): Long? {
        val value = rs.getLong(column)
        return if (rs.wasNull()) null else value
    }

    private fun timestampToOffsetDateTime(timestamp: Timestamp): OffsetDateTime =
        timestamp.toInstant().atOffset(ZoneOffset.UTC)
}
