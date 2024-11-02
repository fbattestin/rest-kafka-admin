
def list(a, args):
    """ list topics, groups and cluster metadata """

    if len(args) == 0:
        what = "all"
    else:
        what = args[0]

    md = a.list_topics(timeout=10)

    print("Cluster {} metadata (response from broker {}):".format(md.cluster_id, md.orig_broker_name))

    if what in ("all", "brokers"):
        print(" {} brokers:".format(len(md.brokers)))
        for b in iter(md.brokers.values()):
            if b.id == md.controller_id:
                print("  {}  (controller)".format(b))
            else:
                print("  {}".format(b))

    if what in ("all", "topics"):
        print(" {} topics:".format(len(md.topics)))
        for t in iter(md.topics.values()):
            if t.error is not None:
                errstr = ": {}".format(t.error)
            else:
                errstr = ""

            print("  \"{}\" with {} partition(s){}".format(t, len(t.partitions), errstr))

            for p in iter(t.partitions.values()):
                if p.error is not None:
                    errstr = ": {}".format(p.error)
                else:
                    errstr = ""

                print("partition {} leader: {}, replicas: {},"
                      " isrs: {} errstr: {}".format(p.id, p.leader, p.replicas,
                                                    p.isrs, errstr))

    if what in ("all", "groups"):
        groups = a.list_groups(timeout=10)
        print(" {} consumer groups".format(len(groups)))
        for g in groups:
            if g.error is not None:
                errstr = ": {}".format(g.error)
            else:
                errstr = ""

            print(" \"{}\" with {} member(s), protocol: {}, protocol_type: {}{}".format(
                g, len(g.members), g.protocol, g.protocol_type, errstr))

            for m in g.members:
                print("id {} client_id: {} client_host: {}".format(m.id, m.client_id, m.client_host))
