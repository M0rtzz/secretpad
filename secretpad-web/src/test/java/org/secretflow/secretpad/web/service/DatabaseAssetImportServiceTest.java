package org.secretflow.secretpad.web.service;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DatabaseAssetImportServiceTest {

    @Test
    void acceptsSelectAndReadOnlyCte() {
        assertDoesNotThrow(() -> DatabaseAssetImportService.validateReadOnlySql(
                "select id, name from customer where status = 'ACTIVE'"));
        assertDoesNotThrow(() -> DatabaseAssetImportService.validateReadOnlySql(
                "with active as (select id from customer) select * from active"));
    }

    @Test
    void rejectsWritesAndMultipleStatements() {
        assertThrows(IllegalArgumentException.class,
                () -> DatabaseAssetImportService.validateReadOnlySql("delete from customer"));
        assertThrows(IllegalArgumentException.class,
                () -> DatabaseAssetImportService.validateReadOnlySql(
                        "with removed as (delete from customer returning id) select * from removed"));
        assertThrows(IllegalArgumentException.class,
                () -> DatabaseAssetImportService.validateReadOnlySql(
                        "select * from customer; drop table customer"));
    }

    @Test
    void ignoresKeywordsInsideStringsAndComments() {
        assertDoesNotThrow(() -> DatabaseAssetImportService.validateReadOnlySql(
                "select 'delete from customer' as sample -- update is text"));
    }
}
