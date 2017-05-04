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
 * Created by Arpan Solanki on 5/4/2017.
 */
public class BeanstalkLogGroupHasSubscription implements ConformityRule {

    private static final Logger LOGGER = LoggerFactory.getLogger(BeanstalkLogGroupHasSubscription.class);

    private final Map<String, AWSClient> regionToAwsClient = Maps.newHashMap();

    private static final String RULE_NAME = "BeanstalkLogGroupHasSubscription";
    private final String reason = "Beanstalk does not have log stream";

    private AWSCredentialsProvider awsCredentialsProvider;

    /**
     * Constructs an instance with the default AWS credentials provider chain.
     * @see com.amazonaws.auth.DefaultAWSCredentialsProviderChain
     */
    public BeanstalkLogGroupHasSubscription() {
        this(new DefaultAWSCredentialsProviderChain());
    }

    /**
     * Constructs an instance with the passed AWS credentials provider.
     * @param awsCredentialsProvider
     *      The AWS credentials provider
     */
    public BeanstalkLogGroupHasSubscription(AWSCredentialsProvider awsCredentialsProvider) {
        this.awsCredentialsProvider = awsCredentialsProvider;
    }

    @Override
    public String getName() {
        return RULE_NAME;
    }

    @Override
    public String getNonconformingReason() {
        return reason;
    }

    @Override
    public Conformity check(Cluster cluster) {
        Set<String> failedBeanstalks = Sets.newHashSet();;
        Collection<String> failedComponents = Lists.newArrayList();
        AWSClient client = getAwsClient(cluster.getRegion());
        if(cluster.getType()=="BEANSTALK") {
            LOGGER.info("cluster type is beanstalk" +cluster.getName());
            boolean hasStream = client.beanstalkHasLogStream(cluster.getName());
            if (!hasStream) {
                LOGGER.info(("beanstalk does not have stream"));
                failedBeanstalks.add(cluster.getName());
            }
            failedComponents.addAll(failedBeanstalks);
        }
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

}
