package com.example.projectstagevermegfinal.front.DTO;

import lombok.Data;

/**
 * Data Transfer Object (DTO) representing customer data.
 */
@Data
public class CustomerDTO {
    private String id;
    private String first_name;
    private String last_name;
    private String gender;
    private String nationality;
    private String email_address;

}
