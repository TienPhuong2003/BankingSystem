package com.banking.transactionservice.service;

import com.banking.transactionservice.client.AccountServiceClient;
import com.banking.transactionservice.config.RedisConfig;
import com.banking.transactionservice.dto.TransactionResponse;
import com.banking.transactionservice.dto.TransferRequest;
import com.banking.transactionservice.entity.Transaction;
import com.banking.transactionservice.entity.TransactionStatus;
import com.banking.transactionservice.entity.TransactionType;
import com.banking.transactionservice.event.TransactionCompletedEvent;
import com.banking.transactionservice.event.TransactionInitiatedEvent;
import com.banking.transactionservice.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionService {
    private final TransactionRepository transactionRepository;
    private final AccountServiceClient accountServiceClient;

    private final KafkaTemplate<String,Object> kafkaTemplate;

    private final RedisTemplate<String, String> redisTemplate;

    private final String TRANSACTION_INITIATED_TOPIC = "transaction.initiated";
    private final String TRANSACTION_COMPLETED_TOPIC = "transaction.completed";
    private final String TRANSACTION_REFUNDED_TOPIC = "transaction.refunded";
    private final String FRAUD_DETECTED_TOPIC = "fraud.detected";

    public TransactionResponse transfer(TransferRequest request){
        log.info("SAGA start - Transfer: {} -> {} amount: {}",
                request.getSenderAccountNumber(),
                request.getReceiverAccountNumber(),
                request.getAmount());
        accountServiceClient.deductBalance(request.getSenderAccountNumber(), request.getAmount());
        Transaction transaction = new Transaction();
        transaction.setSenderAccountNumber(request.getSenderAccountNumber());
        transaction.setReceiverAccountNumber(request.getReceiverAccountNumber());
        transaction.setAmount(request.getAmount());
        transaction.setType(TransactionType.TRANSFER);
        transaction.setStatus(TransactionStatus.PROCESSING);
        transaction.setReferenceNumber(UUID.randomUUID().toString());

        Transaction savedTransaction = transactionRepository.save(transaction);
        log.info("Transaction saved as PROCESSING: {}", savedTransaction.getId());

        TransactionInitiatedEvent event = new TransactionInitiatedEvent(
                savedTransaction.getId(),
                savedTransaction.getSenderAccountNumber(),
                savedTransaction.getReceiverAccountNumber(),
                savedTransaction.getAmount(),
                savedTransaction.getDescription()
        );

        kafkaTemplate.send(TRANSACTION_INITIATED_TOPIC,savedTransaction.getId(), event);
        log.info("Transaction initiated published: {}", savedTransaction.getId());

        return mapToResponse(savedTransaction);
    }

    public TransactionResponse getTransaction(String transactionId){
        return mapToResponse(transactionRepository.findById(transactionId).orElseThrow(() -> new RuntimeException("Transaction not found: " + transactionId)));
    }

    public List<TransactionResponse> getTransactionHistory(String accountNumber){
        return transactionRepository.findBySenderAccountNumberOrderByCreatedAtDesc(accountNumber)
                .stream().map(this::mapToResponse).collect(Collectors.toList());
    }

    public TransactionResponse verifyOTP(String transactionId, String otp){
        log.info("OTP verification for transaction {} ", transactionId);

        Transaction transaction = transactionRepository.findById(transactionId).orElseThrow(() -> new RuntimeException("Transaction not found: " + transactionId));

        String otpKey = "verification:otp" + transactionId;
        String storedOtp = redisTemplate.opsForValue().get(otpKey);

        if(storedOtp == null){
            //OTP expired
            log.warn("OTP verification for transaction {} has expired", transactionId);
            compensateTransaction(transaction,"OTP expried - transaction cancelled and amount refunded");
            return mapToResponse(transaction);
        }
        if(!storedOtp.equals(otp)){
            //Block account and refund
            log.warn("Wrong OTP - blocking account and refunding: {}", transactionId);
            redisTemplate.delete(otpKey);
            blockAccountAndCompensate(transaction, "Wrong OTP entered - transaction canclled, " +
                    "account blocked for security");
            return mapToResponse(transaction);
        }

        log.info("OTP verified - completing transaction: {}", transactionId);
        redisTemplate.delete(otpKey);
        completeTransaction(transaction);
        return  mapToResponse(transaction);
    }

    public void processCleanResult(String transactionId){
        Transaction transaction = transactionRepository.findById(transactionId).orElseThrow(() -> new RuntimeException("Transaction not found: " + transactionId));

        if(transaction.getStatus() != TransactionStatus.PROCESSING){
            log.warn("Transaction {} not PROCESSING - skipping", transactionId);
            return;
        }

        completeTransaction(transaction);
    }

    private void compensateTransaction(Transaction transaction, String reason) {
        log.warn("SAGA COMPENSATION - refunding: {} amount {}", transaction.getSenderAccountNumber(),transaction.getAmount());

        //Credit money back to the sender
        accountServiceClient.creditBalance(transaction.getSenderAccountNumber(), transaction.getAmount());
        transaction.setStatus(TransactionStatus.FLAGGED);
        transaction.setFailureReason(reason + "SAGA Compensation executed, amount refunded at " + LocalDateTime.now());
        transactionRepository.save(transaction);

        Map<String, Object> refundEvent = new HashMap<>();
        refundEvent.put("transactionId", transaction.getId());
        refundEvent.put("senderAccountNumber", transaction.getSenderAccountNumber());
        refundEvent.put("amount", transaction.getAmount());
        refundEvent.put("reason", reason);
        kafkaTemplate.send(TRANSACTION_REFUNDED_TOPIC, refundEvent);

        log.info("SAGA COMPENSATION COMPLETED - {} refunded to {}", transaction.getAmount(), transaction.getSenderAccountNumber());

    }


    private void blockAccountAndCompensate(Transaction transaction, String reason) {
        //publish fraud.detected -> account service will block the account
        Map<String, Object> fraudEvent =  new HashMap<>();
        fraudEvent.put("transactionId", transaction.getId());
        fraudEvent.put("accountNumber", transaction.getSenderAccountNumber());
        fraudEvent.put("reason", reason);

        kafkaTemplate.send(FRAUD_DETECTED_TOPIC,transaction.getSenderAccountNumber(),fraudEvent);

        //SAGA COMPENSATION - refund sender
        compensateTransaction(transaction,reason);

    }

    private void completeTransaction(Transaction transaction) {
        transaction.setStatus(TransactionStatus.COMPLETED);
        transaction.setCompletedAt(LocalDateTime.now());
        transactionRepository.save(transaction);

        TransactionCompletedEvent completedEvent = new TransactionCompletedEvent(transaction.getId(),transaction.getSenderAccountNumber(), transaction.getReceiverAccountNumber(), transaction.getAmount(), transaction.getDescription());

        kafkaTemplate.send(TRANSACTION_COMPLETED_TOPIC, completedEvent);

        log.info("SAGA COMPLETED - Transaction {} completed", transaction.getId());
    }



    private TransactionResponse mapToResponse(Transaction transaction){
        TransactionResponse transactionResponse = new TransactionResponse();
        transactionResponse.setId(transaction.getId());
        transactionResponse.setDescription(transaction.getDescription());
        transactionResponse.setAmount(transaction.getAmount());
        transactionResponse.setSenderAccountNumber(transaction.getSenderAccountNumber());
        transactionResponse.setReceiverAccountNumber(transaction.getReceiverAccountNumber());
        transactionResponse.setStatus(transaction.getStatus());
        transactionResponse.setReferenceNumber(transaction.getReferenceNumber());
        transactionResponse.setType(transaction.getType());
        transactionResponse.setFailureReason(transaction.getFailureReason());
        transactionResponse.setCreatedAt(transaction.getCreatedAt());

        return transactionResponse;
    }
}
