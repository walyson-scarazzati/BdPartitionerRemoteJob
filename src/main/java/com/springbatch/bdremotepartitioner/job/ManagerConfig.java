package com.springbatch.bdremotepartitioner.job;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.integration.config.annotation.EnableBatchIntegration;
import org.springframework.batch.integration.partition.RemotePartitioningManagerStepBuilder;
import org.springframework.batch.integration.partition.RemotePartitioningManagerStepBuilderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.integration.channel.DirectChannel;

@Profile("manager")
@Configuration
@EnableBatchProcessing
@EnableBatchIntegration
public class ManagerConfig {
    private static final int GRID_SIZE = 2;

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private RemotePartitioningManagerStepBuilderFactory stepBuilderFactory;

    public Job remotePartitionJob(@Qualifier("migrarPessoaStep") Step migrarPessoaStep,
                                  @Qualifier("migrarDadosBancarios") Step migrarDadosBancariosStep){
        return  jobBuilderFactory
                .get("remotePartitionJob")
                .start(migrarPessoaStep)
                .next(migrarDadosBancariosStep)
                .incrementer(new RunIdIncrementer())
                .build();
    }

    public Step migrarPessoaStep(@Qualifier("pessoaPartitioner") Partitioner partitioner){
        return  stepBuilderFactory
                .get("migrarPessoaStep")
                .partitioner("migrarPessoaStep", partitioner)
                .gridSize(GRID_SIZE)
                .outputChannel(requests())
                .build();
    }

    public Step migrarDadosBancariosStep(@Qualifier("dadosBancariosPartitioner") Partitioner partitioner){
        return  stepBuilderFactory
                .get("migrarDadosBancariosStep")
                .partitioner("migrarDadosBancariosStep", partitioner)
                .gridSize(GRID_SIZE)
                .outputChannel(requests())
                .build();
    }

    @Bean
    public DirectChannel requests(){
        return new DirectChannel();
    }
}
