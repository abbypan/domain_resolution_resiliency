#!/bin/bash
perl domain_resolution_resiliency.pl google.com. 2&> res/metric-final-google.com..log
perl domain_resolution_resiliency.pl qq.com. 2&> res/metric-final-qq.com..log
perl domain_resolution_resiliency.pl amazon.com. 2&> res/metric-final-amazon.com..log
perl domain_resolution_resiliency.pl twitter.com. 2&> res/metric-final-twitter.com..log
perl domain_resolution_resiliency.pl weibo.cn. 2&> res/metric-final-weibo.cn..log
perl domain_resolution_resiliency.pl facebook.com. 2&> res/metric-final-facebook.com..log

