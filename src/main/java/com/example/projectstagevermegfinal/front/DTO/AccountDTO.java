package com.example.projectstagevermegfinal.front.DTO;

import lombok.Data;

/**
 * Data Transfer Object (DTO) representing account data.
 */
@Data
public class AccountDTO {
    private String id;
    private String name;
    private String permitted_denominations;
    private String status;
    private String stakeholder_ids;
    private String arranged_overdraft_limit;
    private String interest_application_day;
}
