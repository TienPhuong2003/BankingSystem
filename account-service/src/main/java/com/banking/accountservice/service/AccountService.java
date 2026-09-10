package com.banking.accountservice.service;


import com.banking.accountservice.dto.AccountResponse;
import com.banking.accountservice.dto.CreateAccountRequest;
import com.banking.accountservice.entity.Account;
import com.banking.accountservice.entity.AccountStatus;
import com.banking.accountservice.entity.AccountType;
import com.banking.accountservice.repository.AccountRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.security.SecureRandom;

@Service
@Slf4j
@RequiredArgsConstructor
public class AccountService {
    private final AccountRepository accountRepository;
    private static SecureRandom random = new SecureRandom();

    public AccountResponse createAccount(CreateAccountRequest request){
        log.info("creating account for: {}", request.getEmail());

        if (accountRepository.existsByEmail(request.getEmail())){
            throw  new RuntimeException("account already exists for email: " + request.getEmail());
        }

        Account account = new Account();
        account.setAccountHolderName(request.getAccountHolderName());
        account.setEmail(request.getEmail());
        account.setPhone(request.getPhone());
        account.setAccountType(request.getAccountType());
        account.setStatus(AccountStatus.ACTIVE);
        account.setBalance(request.getInitialDeposit());
        account.setAccountNumber(generatedAccountNumber());
        account.setDailyTransactionLimit(request.getAccountType() == AccountType.SAVINGS ? new BigDecimal("100000") : new BigDecimal("500000"));

        Account savedAccount = accountRepository.save(account);
        log.info("Account created: {}", savedAccount.getAccountNumber());
        return mapToResponse(savedAccount);
    }


    public AccountResponse getAccount(String accountNumber){
        Account account = accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(() -> new RuntimeException("Account not found for accountNumber: " + accountNumber));
        return mapToResponse(account);
    }

    public BigDecimal getBalance(String accountNumber){
        Account account = accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(() -> new RuntimeException("Account not found for accountNumber: " + accountNumber));
        return account.getBalance();
    }

    public void blockAccount(String accountNumber){
        log.info("Blocking account for accountNumber: {}", accountNumber);

        Account account = accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(() -> new RuntimeException("Account not found for accountNumber: " + accountNumber));
        account.setStatus(AccountStatus.BLOCKED);
        accountRepository.save(account);
        log.info("Account blocked for accountNumber: {}", accountNumber);
    }

    public void deductBalance(String accountNumber, BigDecimal amount){
        log.info("deducting balance {} from accountNumber: {}", accountNumber);
        Account account = accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(() -> new RuntimeException("Account not found for accountNumber: " + accountNumber));

        if(account.getStatus() != AccountStatus.ACTIVE){
            throw  new RuntimeException("Account not active for accountNumber: " + accountNumber);
        }
        if (account.getBalance().compareTo(amount) <= 0){
            throw new RuntimeException("Insufficient balance for account number: " + accountNumber);
        }
        account.setBalance(account.getBalance().subtract(amount));
        accountRepository.save(account);
        log.info("Balance updated. New balance: {}", account.getBalance());
    }

    public void creditBalance(String accountNumber, BigDecimal amount){
        log.info("crediting balance from {} to {}", amount, accountNumber);
        Account account = accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(() -> new RuntimeException("Account not found for accountNumber: " + accountNumber));
        account.setBalance(account.getBalance().add(amount));
        accountRepository.save(account);
        log.info("Balance updated. New balance: {}", account.getBalance());
    }

    private String generatedAccountNumber(){
        String accountNumber;
        do {
            long number = random.nextLong(1_000_000_000_000L);
            accountNumber = String.format("%012d", number);
        } while (accountRepository.existsByAccountNumber(accountNumber));

        return accountNumber;
    }

    private AccountResponse mapToResponse(Account account){
        AccountResponse response = new AccountResponse();
        response.setId(account.getId());
        response.setAccountHolderName(account.getAccountHolderName());
        response.setEmail(account.getEmail());
        response.setPhone(account.getPhone());
        response.setAccountType(account.getAccountType());
        response.setBalance(account.getBalance());
        response.setStatus(account.getStatus());
        response.setAccountNumber(account.getAccountNumber());
        response.setDailyTransactionLimit(account.getDailyTransactionLimit());
        response.setCreatedAt(account.getCreatedAt());
        response.setUpdatedAt(account.getUpdatedAt());

        return response;
    }
}
