# Checklist for production

You've decided to run a cluster in production — great! Before hitting the red button, here's a list of recommendations
for your setup.

1. Decide on whether you need SSDs — you probably do. Sustaining a high rate
(kHz) of IOPS is not possible with spinning hard disks, and transaction throughput
will be low.

  	Note: You may get great performance initially with a hard drive thanks to page caching,
  	*until the total data stored exceeds the amount of RAM available*, after which
  	throughput will crash.

	* _Recommended:_ Use SSDs, especially if you plan on storing more data than there is RAM available
	on the machine, and need high transaction throughput.

1. Ensure the cluster is secure — redis is a popular protocol, and there are bots scanning the
entire internet looking to attack unsecured redis instances. _This is not theoretical_ and we
have seen it happen.

    * **Essential:** Configure your instance with [password authentication](authentication.md) 
    * _Recommended:_ Ensure the relevant ports are blocked from the open internet with a firewall, only
    available within your internal network.
    * Optional: Use a script that periodically verifies the cluster is inaccessible without
    a password, and its ports shut from the open internet.

1. Ensure backups are taken at regular intervals — even though QuarkDB is replicated, _you still need backups_.

    * **Essential:** Set-up a script to take periodic [backups](backup.md).

    * **Essential:** Ensure your backup script will not silently fail. In case of failure, an alarm should be generated.

    * _Recommended:_ Do basic sanity checking of the generated backup using `quarkdb-validate-checkpoint` tool before putting
    into long-term storage.


