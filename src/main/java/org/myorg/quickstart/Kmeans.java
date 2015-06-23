package org.myorg.quickstart;

import java.io.Serializable;
import java.util.Collection;
import org.apache.commons.math.util.FastMath;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;




/**
 * Created by zien on 6/16/15.
 */
public class Kmeans {
    public static void main(String[] args) throws  Exception{

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<point> point = env.readCsvFile("hdfs://10.3.1.2:39100/user/hpds/kmeans_in")
                                                .types(Double.class,Double.class)
                                                .map(new convertTuple());
        DataSet<center> center = env.readCsvFile("hdfs://10.3.1.2:39100/user/hpds/center")
                                                .types(Integer.class, Double.class, Double.class)
                                                .map(new convertTupleCenter());

        IterativeDataSet<center> itr = center.iterate(5);
        DataSet<center> ncenter = point
                                        .map(new minDistance()).withBroadcastSet(itr, "centers")
                                        .groupBy(0).reduce(new sumcenters())
                                        .map(new meancenter());
        DataSet<center> finalcenter =  itr.closeWith(ncenter);
        DataSet<Tuple3<Integer, Double, Double>> output = finalcenter.map(new centerconvert());
        output.writeAsCsv("hdfs://10.3.1.2:39100/user/hpds/outoutout", "\n", " ");
        env.execute("kmeans");


    }

    public static final class centerconvert implements MapFunction<center, Tuple3<Integer, Double, Double>>{
        @Override
        public Tuple3<Integer, Double, Double> map(center c)throws Exception{
            return new Tuple3<Integer, Double, Double>(c.id, c.q, c.w );
        }
    }

    public static final class meancenter implements MapFunction<Tuple3<Integer, point, Double>, center>{
        @Override
        public center map(Tuple3<Integer, point, Double> t)throws Exception{
            return new center(t.f0, t.f1.div(t.f2));
        }
    }

    public static final class sumcenters implements ReduceFunction<Tuple3<Integer, point, Double>>{
        @Override
        public Tuple3<Integer, point, Double> reduce(Tuple3<Integer, point, Double> a,Tuple3<Integer, point, Double> b)throws Exception{

            return new Tuple3<Integer, point, Double>(a.f0,a.f1.sum(b.f1), a.f2+b.f2 );

        }
    }

    public static final class minDistance extends RichMapFunction<point, Tuple3<Integer, point, Double>>{
        public Collection<center> centers;
        @Override
        public void open(Configuration parameters) throws Exception{
            this.centers = getRuntimeContext().getBroadcastVariable("centers");
        }
        @Override
        public Tuple3<Integer, point, Double> map(point p ) throws Exception{
            double mindis = Double.MAX_VALUE;
            int minid = -1;
            double count = 1;
            for(center c : centers){
                double dis = Math.sqrt((c.q - p.q) * (c.q - p.q) + (c.w - p.w) * (c.w - p.w));
                if(dis<mindis){
                    minid = c.id;
                    mindis = dis;
                }
            }
            return new Tuple3<Integer,point,Double>(minid, p, count);

        }
    }


    public static final class convertTuple implements MapFunction<Tuple2<Double,Double>, point>{
        @Override
        public point map(Tuple2<Double,Double> t)throws Exception{
            return new point(t.f0, t.f1);
        }
    }
    public static final class convertTupleCenter implements MapFunction<Tuple3<Integer,Double,Double>, center>{
        @Override
        public center map(Tuple3<Integer,Double,Double> t)throws Exception{
            return new center(t.f0, t.f1, t.f2);
        }
    }


    public static class point implements Serializable {
        public double q,w;

        public point(){}

        public point(double q,double w){
            this.q = q;
            this.w =w;

        }
        public point div(double val) {
            q /= val;
            w /= val;
            return this;
        }

        public point sum(point val){
            q += val.q;
            w += val.w;
            return this;
        }
    }

    public static class center extends point{
        public int id;

        public center(){}
        public center(int id,double q,double w){
            super(q,w);
            this.id =id;
        }
        public center(int id, point p){
            super(p.q,p.w);
            this.id = id;
        }
    }
}
