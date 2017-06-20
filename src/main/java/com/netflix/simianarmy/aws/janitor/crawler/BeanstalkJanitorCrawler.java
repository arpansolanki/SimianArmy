package com.netflix.simianarmy.aws.janitor.crawler;

import com.amazonaws.services.elasticbeanstalk.model.EnvironmentDescription;
import com.netflix.simianarmy.Resource;
import com.netflix.simianarmy.ResourceType;
import com.netflix.simianarmy.aws.AWSResource;
import com.netflix.simianarmy.aws.AWSResourceType;
import com.netflix.simianarmy.client.aws.AWSClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Arpan Solanki on 6/6/2017.
 */
public class BeanstalkJanitorCrawler extends AbstractAWSJanitorCrawler{

    /** The Constant LOGGER. */
    private static final Logger LOGGER = LoggerFactory.getLogger(BeanstalkJanitorCrawler.class);

    public static final String INSTANCE_FIELD_IS_ALT_ENV = "IS_ALT_ENV";

    public BeanstalkJanitorCrawler(AWSClient awsClient) {
        super(awsClient);
    }

    @Override
    public EnumSet<? extends ResourceType> resourceTypes() {
        return EnumSet.of(AWSResourceType.BEANSTALK);
    }

    @Override
    public List<Resource> resources(ResourceType resourceType) {
        if ("BEANSTALK".equals(resourceType.name())) {
            return getBeanstalkResources();
        }
        return Collections.emptyList();
    }

    @Override
    public List<Resource> resources(String... resourceIds) {
        return getBeanstalkResources(resourceIds);
    }

    private List<Resource> getBeanstalkResources(String... beanstalkIds) {
        List<Resource> resources = new LinkedList<Resource>();
        AWSClient awsClient = getAWSClient();
        for(EnvironmentDescription environment : awsClient.describeBeanstalks(beanstalkIds)){
            Resource beanstalkResource = new AWSResource().withId(environment.getEnvironmentId())
                    .withRegion(getAWSClient().region()).withResourceType(AWSResourceType.BEANSTALK)
                    .withLaunchTime(environment.getDateCreated());
            //add url? //other fields?
            if(environment.getCNAME().endsWith("-alt.us-east-1.elasticbeanstalk.com")) {
                beanstalkResource.setAdditionalField(INSTANCE_FIELD_IS_ALT_ENV, "true");
            }else{
                beanstalkResource.setAdditionalField(INSTANCE_FIELD_IS_ALT_ENV, "false");
            }
            resources.add(beanstalkResource);
        }
        return resources;
    }
}
