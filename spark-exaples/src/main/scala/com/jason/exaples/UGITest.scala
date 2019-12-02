package com.jason.exaples

import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.permission.FsAction
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.util.Shell

/**
  * UserGroupInfoTest
  */
object UGITest {
  //kerveros认证登录hadoop，获取当前票据用户以及用户组
  def getugi(stat: FileStatus): Unit = {
    //获取当前user
    val user = UserGroupInformation.getCurrentUser.getShortUserName
    //获取当前user所在的用户组 （他国hdfs gtoups username ）
    val groups = {
      val cmd = new Shell.ShellCommandExecutor(Array("hdfs", "groups", user), null, null, 0L)
      cmd.execute()
      val s = cmd.getOutput
      val arr = s.split(":", -1)
      if (arr.length == 2) {
        arr(1).split("\\s+")
      } else {
        Array.empty[String]
      }
    }
    val rw = "r"

    val mode: FsAction = rw match {
      case "r" if stat.isFile => FsAction.READ
      case "r" if stat.isDirectory => FsAction.READ_EXECUTE
      case "w" if stat.isFile => FsAction.WRITE
      case "w" if stat.isDirectory => FsAction.WRITE_EXECUTE
      case a => throw new UnsupportedOperationException(s"不支持的读写格式： ${a},")
    }
    val perm = stat.getPermission();
    val ugi = UserGroupInformation.getCurrentUser();
    val bool = if (user.equals(stat.getOwner())) {
      perm.getUserAction().implies(mode)
    } else if (groups.contains(stat.getGroup())) {
      perm.getGroupAction().implies(mode)
    } else {
      perm.getOtherAction().implies(mode)
    }
    bool
  }

  def main(args: Array[String]): Unit = {

  }
}
