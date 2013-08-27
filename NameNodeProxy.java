/*  NameNode's proxy ,鍙互鎷︽埅client鐨勬搷浣�
 * 浣滆�锛氭灄鏅虹厹
 * 鐗堟湰:1.0
 * 鏃堕棿锛�013.2.21
 */
package org.apache.hadoop.hdfs.server.namenode;



import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HDFSPolicyProvider;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.UnregisteredDatanodeException;
import org.apache.hadoop.hdfs.protocol.FSConstants.UpgradeAction;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.IncorrectVersionException;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.hdfs.server.namenode.CheckpointSignature;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem.CompleteFileStatus;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeInstrumentation;
import org.apache.hadoop.hdfs.server.namenode.web.resources.NamenodeWebHdfsMethods;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.UpgradeCommand;
import org.apache.hadoop.hdfs.web.AuthFilter;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.hdfs.web.resources.Param;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.RefreshUserMappingsProtocol;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.authorize.RefreshAuthorizationPolicyProtocol;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;


public class NameNodeProxy implements ClientProtocol ,DatanodeProtocol,
                                      NamenodeProtocol,RefreshAuthorizationPolicyProtocol,
                                     RefreshUserMappingsProtocol {
	 public static final Log LOG = LogFactory.getLog(NameNodeProxy.class.getName());
	 public static final String SEPARATOR1 = "/";
	 public static final String SEPARATOR2 = ".";
       private NameNode namenode;
     // private static Configuration conf=new Configuration();
     
       public NameNodeProxy(NameNode nn){
    	   this.namenode=nn;
       }
       
       
       // ClientProtocol interface
       public LocatedBlocks   getBlockLocations(String src, 
               long offset, 
               long length) throws IOException {
    	   return namenode.getBlockLocations(src, offset, length);
       }
       
       @Deprecated
       public void create(String src, 
                          FsPermission masked,
                                  String clientName, 
                                  boolean overwrite,
                                  short replication,
                                  long blockSize
                                  ) throws IOException {
    	   namenode.create(src, masked, clientName, overwrite, replication, blockSize);
    	   
    	   detection(src,"creat");
       }
       public void create(String src, 
               FsPermission masked,
                       String clientName, 
                       boolean overwrite,
                       boolean createParent,
                       short replication,
                       long blockSize
                       ) throws IOException {
    	   namenode.create(src, masked, clientName, overwrite, createParent, replication, blockSize);
    	   //Configuration conf = new Configuration();
    	   //conf.addResource("monitor.xml");
    	  // LOG.info(src);
    	  // LOG.info(conf.get("dir")+conf.get("format"));
    	   detection(src,"creat");
       }
       public LocatedBlock append(String src, String clientName) throws IOException {
    	   return namenode.append(src, clientName);
       }
       
       public boolean setReplication(String src, 
               short replication
               ) throws IOException {
    	   return namenode.setReplication(src, replication);
       }
       
       public void setPermission(String src, FsPermission permissions
    		      ) throws IOException {
    	   namenode.setPermission(src, permissions);
       }
       
       public void setOwner(String src, String username, String groupname
    		      ) throws IOException {
    	   namenode.setOwner(src, username, groupname);
       }
       
       public void abandonBlock(Block b, String src, String holder
    		      ) throws IOException {
    	   namenode.abandonBlock(b, src, holder);
       }
       
       public LocatedBlock addBlock(String src, 
               String clientName) throws IOException {
           return namenode.addBlock(src, clientName);
       }

       public LocatedBlock addBlock(String src,
               String clientName,
               DatanodeInfo[] excludedNodes) throws IOException {
    	   return namenode.addBlock(src, clientName, excludedNodes);
       }
       
       public boolean complete(String src, String clientName) throws IOException {
    	   return namenode.complete(src, clientName);
       }
       
       public boolean rename(String src, String dst) throws IOException {
    	   return namenode.rename(src, dst);
       }
       
       public boolean delete(String src, boolean recursive) throws IOException {
    	   return namenode.delete(src, recursive);
       }
       
       @Deprecated
       public boolean delete(String src) throws IOException {
         return namenode.delete(src, true);
       }
       
       public boolean mkdirs(String src, FsPermission masked) throws IOException {
    	   return namenode.mkdirs(src, masked);
       }
       
       public DirectoryListing getListing(String src, byte[] startAfter)
    		   throws IOException {
    	   return namenode.getListing(src, startAfter);
       }
       
       public void renewLease(String clientName) throws IOException {
    	   namenode.renewLease(clientName);
       }
       
       public boolean recoverLease(String src, String clientName) throws IOException {
    	   return namenode.recoverLease(src, clientName);
       }
       public long[] getStats() throws IOException {
    	   return namenode.getStats();
       }
       
       public DatanodeInfo[] getDatanodeReport(DatanodeReportType type)
    		   throws IOException {
    	   return namenode.getDatanodeReport(type);
       }
       
       public long getPreferredBlockSize(String filename) throws IOException{
    	   return namenode.getPreferredBlockSize(filename);
       }
       
       public boolean setSafeMode(FSConstants.SafeModeAction action) throws IOException{
    	   return namenode.setSafeMode(action);
       }
       
       public void saveNamespace() throws IOException{
    	   namenode.saveNamespace();
       }
       
       public void refreshNodes() throws IOException{
    	   namenode.refreshNodes();
       }
       
       public void finalizeUpgrade() throws IOException{
    	   namenode.finalizeUpgrade();
       }
       
       public UpgradeStatusReport distributedUpgradeProgress(UpgradeAction action) 
    		   throws IOException{
    	   return namenode.distributedUpgradeProgress(action);
       }
       
       public void metaSave(String filename) throws IOException{
    	   namenode.metaSave(filename);
       }
       
       public void setBalancerBandwidth(long bandwidth) throws IOException{
    	   namenode.setBalancerBandwidth(bandwidth);
       }
       
       public HdfsFileStatus getFileInfo(String src) throws IOException{
    	   return namenode.getFileInfo(src);
       }
       
       public ContentSummary getContentSummary(String path) throws IOException{
    	   return namenode.getContentSummary(path);
       }
       
       public void setQuota(String path, long namespaceQuota, long diskspaceQuota)
               throws IOException{
    	   namenode.setQuota(path, namespaceQuota, diskspaceQuota);
       }
       
       public void fsync(String src, String client) throws IOException{
    	   namenode.fsync(src, client);
       }
       
       public void setTimes(String src, long mtime, long atime) throws IOException{
    	   namenode.setTimes(src, mtime, atime);
       }
       
       public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer) throws IOException{
    	   return namenode.getDelegationToken(renewer);
       }
       
       public long renewDelegationToken(Token<DelegationTokenIdentifier> token)
    		      throws IOException{
    	   return namenode.renewDelegationToken(token);
       }
       
       public void cancelDelegationToken(Token<DelegationTokenIdentifier> token)
    		      throws IOException{
    	   namenode.cancelDelegationToken(token);
       }
       
       // DataNodeProtocol interface
       
       public DatanodeRegistration register(DatanodeRegistration nodeReg
               ) throws IOException {
    	   return namenode.register(nodeReg);
       }
       
       public DatanodeCommand[] sendHeartbeat(DatanodeRegistration nodeReg,
               long capacity,
               long dfsUsed,
               long remaining,
               int xmitsInProgress,
               int xceiverCount) throws IOException {
    	   return namenode.sendHeartbeat(nodeReg, capacity, dfsUsed, remaining, xmitsInProgress, xceiverCount);
       }
       
       public DatanodeCommand blockReport(DatanodeRegistration nodeReg,
               long[] blocks) throws IOException {
    	   return namenode.blockReport(nodeReg, blocks);
       }
       
       public void blocksBeingWrittenReport(DatanodeRegistration registration,
    		      long[] blocks) throws IOException{
    	   namenode.blocksBeingWrittenReport(registration, blocks);
       }
       
       public void blockReceived(DatanodeRegistration nodeReg, 
               Block blocks[],
               String delHints[]) throws IOException {
    	   namenode.blockReceived(nodeReg, blocks, delHints);
       }
       
       public void errorReport(DatanodeRegistration nodeReg,
               int errorCode, 
               String msg) throws IOException {
    	   namenode.errorReport(nodeReg, errorCode, msg);
       }
       
       public void reportBadBlocks(LocatedBlock[] blocks) throws IOException{
    	   namenode.reportBadBlocks(blocks);
       }
       public long nextGenerationStamp(Block block, boolean fromNN) throws IOException{
    	   return namenode.nextGenerationStamp(block, fromNN);
       }
       public void commitBlockSynchronization(Block block,
    		      long newgenerationstamp, long newlength,
    		      boolean closeFile, boolean deleteblock, DatanodeID[] newtargets
    		      ) throws IOException{
    	   namenode.commitBlockSynchronization(block, newgenerationstamp, newlength, closeFile, deleteblock, newtargets);
       }
       public NamespaceInfo versionRequest() throws IOException {
    	   return namenode.versionRequest();
       }
       
       public UpgradeCommand processUpgradeCommand(UpgradeCommand comm) throws IOException {
    	   return namenode.processUpgradeCommand(comm);
       }
       
       // NamenodeProtocol interface
       public BlocksWithLocations getBlocks(DatanodeInfo datanode, long size)
    		   throws IOException{
    	   return namenode.getBlocks(datanode, size);
       }
       
       public ExportedBlockKeys getBlockKeys() throws IOException{
    	   return namenode.getBlockKeys();
       }
       
       public long getEditLogSize() throws IOException{
    	   return namenode.getEditLogSize();
       }
       
       public CheckpointSignature rollEditLog() throws IOException{
    	   return namenode.rollEditLog();
       }
       
       public void rollFsImage() throws IOException{
    	   namenode.rollFsImage();
       }
       
       // RefreshUserMappingsProtocol interface
       public void refreshUserToGroupsMappings() throws IOException{
    	   namenode.refreshUserToGroupsMappings();
       }
       
       public void refreshSuperUserGroupsConfiguration() 
    		   throws IOException{
    	   namenode.refreshSuperUserGroupsConfiguration();
       }
       
       //RefreshAuthorizationPolicyProtocol interface
       public void refreshServiceAcl() throws IOException{
    	   namenode.refreshServiceAcl();
       }
       
       // add method
       private void detection(String src, String cmd) throws IOException{
    	   Configuration conf=new Configuration();
    	   conf.addResource("monitor.xml");
    	   int slash1 = src.lastIndexOf(SEPARATOR1);    	
    	   int slash2 = src.lastIndexOf(SEPARATOR2);
    	 //  LOG.info(conf.get("dir")+conf.get("format"));
    	   String s1=src.substring(0, slash1);
    	   String s2=src.substring(slash2+1);
    	   String s3=src.substring(slash1+1);
    	 // LOG.info(s1);
    	 // LOG.info(s2);
    	//  LOG.info(src.substring(0, slash1)+"test");
    	   if(s1.equals(conf.get("dir"))){
    		  // LOG.info(src.substring(1, slash1));
    		   if(s2.equals(conf.get("format"))){
    			   //Runtime.getRuntime().exec(new String[]("sh"+"/hadoop/conf/algorithm.sh");
    			   Runtime.getRuntime().exec(new String[]{"hadoop","jar","/hadoop/lzy.jar","lzy."+conf.get("algorithm"),src,s3+"-output"});
    			   //System.out.print(conf.get("dir")+conf.get("format"));
    			   LOG.info(src+" "+conf.get("dir")+" "+conf.get("format"));
    		   }
    	   }
    	   
       }


	@Override
	public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
		// TODO Auto-generated method stub
		return namenode.getProtocolVersion(protocol, clientVersion);
	}


	
}
