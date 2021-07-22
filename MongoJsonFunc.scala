package main

import com.mongodb.{BasicDBList, BasicDBObject, DBObject}
import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * <dependency>
  * <groupId>org.mongodb</groupId>
  * <artifactId>mongo-java-driver</artifactId>
  * <version>3.4.1</version>
  * </dependency>
  *
  * 展开json, 支持两种模式:
  * explode: 下钻模式, key1.key2.key3 返回key3对应的value
  * flatten: 平展模式, key1.key2.key3 会一层层将外层的数据平展到下层
  * key2一层会多出key1层除了key2的以外的数据,
  * key3层会将上面key2的数据平展到key3层
  */
object MongoJsonFunc extends App {

    //从最终的DBObject中 过滤需要的字段
    //lateral view explode(a.b.c) s as x, y , z
    //处理 as 之后的元素, 选出 x, y, z
    def filterAsColumn(last: Seq[DBObject], view: Seq[String]): List[Map[String, Any]] = {
        val x = last.map { item =>
            item match {
                case bl: BasicDBList =>
                    bl.map(_.asInstanceOf[BasicDBObject])
                            .map { o =>
                                view match {
                                    case Seq("*") =>
                                        o.map(x => (x._1, if (x._2 == null) "" else x._2)).toMap
                                    case _ => view.map(a => (a, o.get(a))).toMap
                                }
                            }.toList

                case bo: BasicDBObject =>
                    view match {
                        case Seq("*") =>
                            bo.map(x => (x._1, if (x._2 == null) "" else x._2)).toMap :: Nil
                        case _ =>
                            view.map(a => (a, bo.get(a)))
                                    .toMap :: Nil
                    }

                case _ => List.empty
            }
        }
        x.flatten.toList
    }

    //给定一个lateral view 和 一个mongo doc
    //select * from db.table lateral view explode(a.b.c) s as x, y
    //展开view的columns(a.b.c), 拿到doc中最深层的那个列,将其展开,拿出具体的 x, y
    //组装成List[Map[x->v]]
    def explodeLateralViewTable(cc: Seq[String], dbo: DBObject, view: Seq[String]): List[Map[String, Any]] = {
        //展开column (a.b.c) 从 DBObject 中依次取到 a -> b ->c
        def iteraColumn(cs: Seq[String], o: DBObject): Seq[DBObject] = {
            def iteraDBObject(dbo: DBObject, x: String): Seq[DBObject] = dbo match {
                case ob: BasicDBObject => ob.get(x).asInstanceOf[DBObject] :: Nil
                case ol: BasicDBList => ol.map { oli =>
                    oli match {
                        case olio: BasicDBObject => olio.get(x).asInstanceOf[DBObject] :: Nil
                        case olil: BasicDBList =>
                            iteraDBObject(olil.asInstanceOf[DBObject], x)
                    }
                }.flatten
            }

            cs match {
                case Seq(s) if (s == "*") => Seq(o)
                case Seq(x) => iteraDBObject(o, x)
                case Seq(head, tail@_*) => iteraColumn(tail, o.get(head).asInstanceOf[DBObject])
            }
        }

        val last = iteraColumn(cc, dbo)
        filterAsColumn(last, view)
    }

    def flattenLateralViewTable(flattenStr: Seq[String], dbo: Seq[DBObject], view: Seq[String]): List[Map[String, Any]] = {
        var flattenSeq = flattenStr
        var originDBO = dbo
        val result = mutable.ListBuffer[Map[String, Any]]()

        def flatten(targetDBO: DBObject, noTargetColumns: mutable.Map[String, AnyRef]): Seq[DBObject] = {
            targetDBO match {
                case dbl: BasicDBList => dbl.map(x => flatten(x.asInstanceOf[DBObject], noTargetColumns)).flatten
                case o: BasicDBObject =>
                    noTargetColumns.foreach { case (k, v) =>
                        o.append(k, v)
                    }
                    Seq(o)
            }
        }

        def explode(o: DBObject, head: String): Seq[DBObject] = o match {
            case dbl: BasicDBList => dbl.map(x => explode(x.asInstanceOf[DBObject], head)).flatten
            case bo: BasicDBObject =>
                val targetDBO = bo.get(head).asInstanceOf[DBObject]
                val noTargetColumns = bo.filterNot(_._1 == head)
                if (targetDBO != null) {
                    flatten(targetDBO, noTargetColumns)
                    Seq(targetDBO)
                } else {
                    Seq.empty
                }
        }

        def collect(o: Seq[DBObject]): Unit = o.foreach {
            case list: BasicDBList => list.foreach { x =>
                val tmp = x.asInstanceOf[DBObject]
                collect(Seq(tmp))
            }
            case o: BasicDBObject =>
                val map = o.toMap.map(x => (x._1.toString, x._2)).toMap
                result.append(map)
        }

        while (flattenSeq != null && !flattenSeq.isEmpty) {
            val head = flattenSeq.head
            val seqDBO = originDBO.map(explode(_, head)).flatten
            originDBO = seqDBO
            flattenSeq = flattenSeq.tail
        }
        //collect(originDBO)
        //result.toList
        filterAsColumn(originDBO, view)
    }

    val json =
        """
          |{
          |  success : 0,
          |  errorMsg : "错误消息",
          |  data : {
          |   total : "总记录数",
          |   rows : [ {
          |    id : "任务ID",
          |    workName : "任务名称",
          |    assigneeName : "经办人姓名",
          |    name : "流程步骤名称",
          |    processInstanceInitiatorName : "发起人",
          |    processInstanceStartTime : "发起时间",
          |    createTime : "到达时间",
          |    dueDate : "截止时间"
          |   }, {
          |    id : "ID",
          |    workName : "名称",
          |    assigneeName : "经办人",
          |    name : "流程",
          |    processInstanceInitiatorName : "发起人",
          |    processInstanceStartTime : "发起",
          |    createTime : "到达",
          |    dueDate : "截止"
          |   } ]
          |  }
          | }
        """.stripMargin

    val dbo = BasicDBObject.parse(json)

    //
    val find = "data.rows"
    //剪裁列
    val view = Seq("success","errorMsg","name", "createTime", "workName")
    val res = explodeLateralViewTable(find.split("\\.").toSeq, dbo, view)
    println(res)
    val res2 = flattenLateralViewTable(find.split("\\.").toSeq, dbo :: Nil, view)
    println(res2)
}

