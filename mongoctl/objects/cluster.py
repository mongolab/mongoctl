__author__ = 'abdul'

from base import DocumentWrapper




###############################################################################
# Generic Cluster Class
###############################################################################
class Cluster(DocumentWrapper):

    ###########################################################################
    # Constructor and other init methods
    ###########################################################################
    def __init__(self, cluster_document):
        DocumentWrapper.__init__(self, cluster_document)
        self._members = self._resolve_members("members")

    ###########################################################################
    def _resolve_members(self, member_prop):
        member_documents = self.get_property(member_prop)
        members = []

        # if members are not set then return
        member_type = self.get_member_type()
        if member_documents:
            for mem_doc in member_documents:
                member = member_type(mem_doc)
                members.append(member)

        return members

    ###########################################################################
    def get_member_type(self):
        raise Exception("Should be implemented by subclasses")

    ###########################################################################
    # Properties
    ###########################################################################
    def get_description(self):
        return self.get_ignore_str_property("description")

    ###########################################################################
    def get_members(self):
        return self._members

    ###########################################################################
    def get_servers(self):
        servers = []
        for member in self.get_members():
            if member.get_server():
                servers.append(member.get_server())

        return servers

    ###########################################################################
    def get_members_info(self):
        info = []
        for member in self.get_members():
            server = member.get_server()
            if server is not None:
                info.append(server.id)
            else:
                info.append("<Invalid Member>")

        return info

    ###########################################################################
    def get_repl_key(self):
        return self.get_property("replKey")

    ###########################################################################
    def has_member_server(self, server):
        return self.get_member_for(server) is not None

    ###########################################################################
    def get_member_for(self, server):
        for member in self.get_members():
            if (member.get_server() and
                        member.get_server().id == server.id):
                return member

        return None

    ###########################################################################
    def get_status(self):
        """
            Needs to be overridden
        """
    ###########################################################################
    def get_default_server(self):
        """
            Needs to be overridden
        """

    ###########################################################################
    def is_auth(self):
        if self.get_repl_key():
            return True
        else:
            auth_servers = filter(lambda s: s.is_auth(), self.get_servers())
            return auth_servers and len(auth_servers) > 0

    ###########################################################################
    def get_mongo_uri_template(self, db=None):

        auth = self.is_auth()
        if not db:
            if auth:
                db = "[/<dbname>]"
            else:
                db = ""
        else:
            db = "/" + db

        server_uri_templates = []
        for server in self.get_servers():
            if server.is_cluster_connection_member():
                server_uri_templates.append(server.get_address_display())

        creds = "[<dbuser>:<dbpass>@]" if auth else ""
        return ("mongodb://%s%s%s" % (creds, ",".join(server_uri_templates),
                                      db))
