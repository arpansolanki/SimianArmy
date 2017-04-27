package com.netflix.simianarmy.aws.conformity.rule;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.simianarmy.client.aws.AWSClient;
import com.netflix.simianarmy.conformity.Cluster;
import com.netflix.simianarmy.conformity.Conformity;
import com.netflix.simianarmy.conformity.ConformityRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Created by Arpan Solanki on 4/26/2017.
 */
public class DynamoTableHasStream implements ConformityRule {

    private static final Logger LOGGER = LoggerFactory.getLogger(InstanceInVPC.class);

    private final Map<String, AWSClient> regionToAwsClient = Maps.newHashMap();

    private AWSCredentialsProvider awsCredentialsProvider;

    private static final String RULE_NAME = "DynamodbTableHasStream";
    private static final String REASON = "Stream not defined";

    /**
     * Constructs an instance with the default AWS credentials provider chain.
     * @see com.amazonaws.auth.DefaultAWSCredentialsProviderChain
     */
    public DynamoTableHasStream() {
        this(new DefaultAWSCredentialsProviderChain());
    }

    /**
     * Constructs an instance with the passed AWS credentials provider.
     * @param awsCredentialsProvider
     *      The AWS credentials provider
     */
    public DynamoTableHasStream(AWSCredentialsProvider awsCredentialsProvider) {
        this.awsCredentialsProvider = awsCredentialsProvider;
    }

    @Override
    public Conformity check(Cluster cluster) {
        Set<String> failedTables = Sets.newHashSet();;
        Collection<String> failedComponents = Lists.newArrayList();
        AWSClient client = getAwsClient(cluster.getRegion());
        if(cluster.getType()=="DYNAMODB_TABLE") {
            boolean hasStream = client.dynamoTableHasStream(cluster.getName());
            if (!hasStream) {
                failedTables.add(cluster.getName());
            }
            failedComponents.addAll(failedTables);
        }//end of if for dynamodb tables
        return new Conformity(getName(), failedComponents);
    }

    private AWSClient getAwsClient(String region) {
        AWSClient awsClient = regionToAwsClient.get(region);
        if (awsClient == null) {
            awsClient = new AWSClient(region, awsCredentialsProvider);
            regionToAwsClient.put(region, awsClient);
        }
        return awsClient;
    }

    @Override
    public String getName() {
        return RULE_NAME;
    }

    @Override
    public String getNonconformingReason() {
        return REASON;
    }
}
