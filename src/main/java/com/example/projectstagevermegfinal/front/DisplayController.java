package com.example.projectstagevermegfinal.front;

import com.example.projectstagevermegfinal.front.DTO.AccountDTO;
import com.example.projectstagevermegfinal.front.DTO.CustomerDTO;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.List;

/**
 * Controller class responsible for handling display-related requests.
 */
@Controller
public class DisplayController {

    private final DisplayService displayService;

    public DisplayController(DisplayService displayService) {
        this.displayService = displayService;

    }

    /**
     * Handles the display of customer data.
     *
     * @param model The Spring Model used to pass data to the view.
     * @return The name of the Thymeleaf template to render.
     */
    @GetMapping("/customers")
    public String displayCustomers(Model model) {
        List<CustomerDTO> customerDTOs = displayService.getCustomers();
        model.addAttribute("customerDTOs", customerDTOs);
        return "customers";
    }

    /**
     * Handles the display of customer accounts for a specific customer.
     *
     * @param customerId The ID of the customer.
     * @param model      The Spring Model used to pass data to the view.
     * @return The name of the Thymeleaf template to render.
     */

    @GetMapping("/customer/accounts/{customerId}")
    public String displayCustomerAccounts(@PathVariable String customerId, Model model) {
        List<AccountDTO> accountDTOs = displayService.getCustomerAccounts(customerId);
        String customerName = displayService.getCustomerName(customerId);
        model.addAttribute("accountDTOs", accountDTOs);
        model.addAttribute("customerName", customerName);
        model.addAttribute("customerId", customerId); // Add customer ID to the model
        return "customerAccounts";
    }

    /**
     * Handles the AJAX request to refresh customer accounts data.
     *
     * @param customerId The ID of the customer.
     * @return The HTML representation of customer accounts data.
     */

    @GetMapping("/refreshDataAccounts")
    @ResponseBody
    public String refreshDataAccounts(@RequestParam String customerId) {
        List<AccountDTO> accountDTOs = displayService.getCustomerAccounts(customerId);
        StringBuilder responseBuilder = new StringBuilder();

        // Build the response HTML based on the accountDTOs
        for (AccountDTO accountDTO : accountDTOs) {
            responseBuilder.append("<tr>");
            responseBuilder.append("<td>").append(accountDTO.getId()).append("</td>");
            responseBuilder.append("<td>").append(accountDTO.getName()).append("</td>");
            responseBuilder.append("<td>").append(accountDTO.getPermitted_denominations()).append("</td>");
            responseBuilder.append("<td>").append(accountDTO.getStatus()).append("</td>");
            responseBuilder.append("<td>").append(accountDTO.getStakeholder_ids()).append("</td>");
            responseBuilder.append("<td>").append(accountDTO.getArranged_overdraft_limit()).append("</td>");
            responseBuilder.append("<td>").append(accountDTO.getInterest_application_day()).append("</td>");
            responseBuilder.append("</tr>");
        }

        // Return the constructed response HTML
        return responseBuilder.toString();
    }

    /**
     * Handles the AJAX request to refresh customer data.
     *
     * @return The HTML representation of customer data.
     */

    @GetMapping("/refreshDataCustomer")
    @ResponseBody
    public String refreshDataCustomers() {
        List<CustomerDTO> customerDTOs = displayService.getCustomers();
        StringBuilder responseBuilder = new StringBuilder();

        // Build the response HTML based on the customerDTOs
        for (CustomerDTO customerDTO : customerDTOs) {
            responseBuilder.append("<tr>");
            responseBuilder.append("<td>").append(customerDTO.getId()).append("</td>");
            responseBuilder.append("<td>").append(customerDTO.getFirst_name()).append("</td>");
            responseBuilder.append("<td>").append(customerDTO.getLast_name()).append("</td>");
            responseBuilder.append("<td><a href='/customer/accounts/").append(customerDTO.getId()).append("'>View Accounts</a></td>");
            responseBuilder.append("</tr>");
        }
        return responseBuilder.toString();
    }



}
