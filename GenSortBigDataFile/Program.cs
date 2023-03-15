using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime;
using System.Linq;
using System.Diagnostics;
using System.IO;
using System.Threading;


/// <summary>
/// Суть задания: сгенерировать огромный файл размером до 300 Гб
/// в формате 123. строка_разной_длины
/// и отсортировать его
/// https://www.youtube.com/watch?v=8XIMleu0YWI
/// </summary>


class StringData
{

    /// <summary>
    /// Максимальная длина строки
    /// </summary>
    private int SIZE = 2000; 

    /// <summary>
    /// Порядковый номер строки
    /// </summary>
    /// 

    public int Num { get; set; }
    /// <summary>
    /// Строка данных
    /// </summary>
    public string Data { get; set; }

    /// <summary>
    /// Дает значение данных в форме строки 
    /// </summary>
    public string Get => (Num.ToString() + ". " + Data);

    /// <summary>
    /// Присваивание значений
    /// </summary>
    /// <param name="value"></param>
    public string change
    {
        set
        {
            if (value == null) 
            {
                Console.WriteLine("Пытались передать {0}",value);
                throw new Exception ("Пытаются передать битые данные! ");
                 }
            try
            {
                int p = value.IndexOf(". ");
                Num = int.Parse(value.Substring(0, p)); // число - номер строки
                Data = value.Substring(p + 2); // данные
            }
            finally { }
        }
    }

    /// <summary>
    /// new_item - просто новый элемент
    /// </summary>
    public void new_item (int i)
    {
        Random r = new Random(new Random().Next(SIZE) + 100);
        
        Num = i; // число - номер строки
        char[]  T = new char[r.Next(50+r.Next(1,10),SIZE)];
        T = T.AsParallel().Select( x => (char) r.Next('A', 'Z') ).ToArray();
        Data = new string(T, 0, T.Length) ;// данные
        
        
        //можно удалить, добавка для качественной генерации ЦП 5%
        if (r.Next(1, 1700) == 17) { Thread.Sleep(1 + r.Next(1, 4)); Console.Write($"Данные генерируются {i} \r",i); }

    }
    public StringData(int N, string D)
    {
        Num = N;
        // прямое наполнение
        Data = D;
    }

    public StringData(int N = 0)
    {
        Num = N;
        new_item(Num);
    }

    public StringData()
    {
        Num = 0;
        Data = "";
    }
}

/// <summary>
/// Сравнивалка типа данных
/// </summary>
class MyDataComparasion
{
    public int MyDataComparate ( StringData a, StringData b)
    {
        //Быстро сравнение
        int res = 0; //равны по умолчанию
        try
        {
            int i = 0;
            int len = a.Data.Length;
            if (len > b.Data.Length) { len = b.Data.Length; }

            //цикл сравнения по символам

            while (i < len)
            {
                char x = a.Data[i];
                char y = b.Data[i];
                if (x < y)
                {
                    res = -1;
                    break; //ускоритель
                }
                else
                if (x > y)
                {
                    res = 1;
                    break; //выходим когда стало понятно, что более или менее
                }
                i++;//идем по символам, по порядку

            }//while
        } //try
        catch
        {

        }//catch
        finally
        {
            //если строки вдруг одинатковы то по номерку
            if (res == 0)
            {
                int x = a.Num;
                int y = b.Num;
                if (x < y) { res = -1; }
                if (x > y) { res = 1; }
            }
        }

        
        //говорим, что больше, или что меньше
        return res;
    }
}


/// <summary>
/// Сортировщик строк в файлах
/// </summary>
class SortStringData
{
    /// <summary>
    /// 
    /// </summary>
    /// <param name="filename"> Имя файла с данными </param>
    /// <param name="chunck_len">Кол-во строк необходимое для частичной сортировки </param>
    public SortStringData(string filename, int chunck_len = 100)
    {
        //Буфер из строк
        List<StringData> core = new List<StringData>();
        // читаем файл
        System.IO.StreamReader sr= new System.IO.StreamReader(filename);

        core.Clear();
        
        //номер куска кэша
        int file_chunk_id = 0;


        while (!sr.EndOfStream)
        {
            //Номер куска кэша 1, 2 , 3 .....
            file_chunk_id++;

            // 1 считываем кусок даннных
            for (int i = 0; i <= chunck_len; i++)
            {

                string str = sr.ReadLine();
                // поток иссяк - на выход
               
                StringData real_string = new StringData();
                real_string.change = str;
                
                // тут проверить как нормально ли идет прием строк
                core.Add(real_string);

                if (sr.EndOfStream)
                {
                    break; // всё!
                }
            }  ;
            
            // 2 Теперь сортируем, то что получили
            core.Sort( new Comparison<StringData>(delegate (StringData a, StringData b) { return (new MyDataComparasion().MyDataComparate(a,b));   } ));

            // 3 сбрасываем в кэш
            string OUT_CACHE_CHUNK_FILENAME = filename + "." + file_chunk_id.ToString();
            //очищаем файл
            System.IO.File.WriteAllText(OUT_CACHE_CHUNK_FILENAME, "");
            //выливаем файл
            System.IO.StreamWriter sw = System.IO.File.AppendText(OUT_CACHE_CHUNK_FILENAME);
            foreach (StringData ix in core)
                {
                sw.WriteLine(ix.Get);
                 } //foreach
            sw.Close();
            core.Clear();
            GC.Collect();

            if (sr.EndOfStream)
            {
                break; // всё!
            }


        }
        sr.Close();
        // ==== сортировка ====

    }
}


class SortViaCache
{


    /// <summary>
    /// Сортировщик данных из кэша
    /// 1 Собирает данные из файла .1 и т.п. в массив 
    /// 2 Сортирует его по порядку
    /// 3 Выводит в новый файл - уже полностью отсортированный
    /// </summary>
    /// <param name="filename">Имя файла послужившего названием кэша (без .0 .1)</param>
    /// <param name="chunk_count"> Кол-во файлов к кэше .1 ... .n - обработка идет построчно </param>
    /// <param name="SIZE">Кол-во строк в массиве </param>
    public SortViaCache(string filename, int chunk_count , int SIZE  )
    {
        //Список кусков кэша
        List<string> filenames = new List<string>();
        List<System.IO.StreamReader> streams = new List<System.IO.StreamReader>();

        string sorted_data_filename = filename + ".sorted.txt";
        
        //очистка выходного файла
        System.IO.File.WriteAllText(sorted_data_filename, "");

        filenames.Clear();
        streams.Clear();

        for (int i = 1; i <= chunk_count; i++)
        {
            string FN_cache = filename + "." + i.ToString();
            filenames.Add(FN_cache);
            streams.Add(new System.IO.StreamReader(FN_cache));
        }

        //--- Список отсортированных данных
        List<StringData> core = new List<StringData>();

        int BLOCK_SIZE = SIZE/(chunk_count/2)  + 1;
        core.Clear();
        int closed = 0;

        for (int k = 0; k < SIZE / BLOCK_SIZE+1; k++)
        {
            for (int j = 0; j <= BLOCK_SIZE; j++)
            {
                //считываение данных из файла идет последовательно
                // но немного будем считывать с запасом, т.к. некоторые данные получаются недосортированными

                //если все потоки закрыты то выходим!
                if (closed >= chunk_count)
                { break; }

                closed = 0; // не вышли посчитаем снова

                for (int i = 0; i < chunk_count; i++)
                {
                    //защита от разных длинной потоков
                    if (streams[i] == null)
                    {
                        closed++; //поток закрыт
                        continue;
                    }

                    // подготавливаем строку данных
                    StringData strD = new StringData();

                    //считываем строку из потока
                    string tmp_str = streams[i].ReadLine();

                    //обновляем строку
                    strD.change = tmp_str;

                    //загоняем данные в лист для сортировки
                    core.Add(strD);

                    if (streams[i].EndOfStream)
                    {
                        //Закрываем и чистим
                        streams[i].Close();
                        streams[i].Dispose();
                        streams[i] = null;
                    }
                } //for
            }//for j



            /// Шаг 2 - сортировка
            Console.Write("Сортировка...");
            core.Sort(new Comparison<StringData>(delegate (StringData a, StringData b) { return (new MyDataComparasion().MyDataComparate(a, b)); }));
            Console.WriteLine(" блок {0}",k) ;

            // Шаг 3 Сброс в файл
            // 
            Console.Write("Пишем часть данных...");
            StreamWriter sw = System.IO.File.AppendText(sorted_data_filename);
            foreach (StringData s in core)
            {

                sw.WriteLine(s.Get);

            } //foreach выгрузка данных
            sw.Close();
            Console.WriteLine(" готов блок {0}", k);

            //Очистка буфера
            core.Clear();

        } //for k


        }//SortViaCache
    }



namespace GenSortBigDataFile
{
    class Program
    {
        static void Main(string[] args)
        {

            Random r = new Random(100 + new Random().Next (13, 100));
            r.Next();

            int SIZE = 2500_071; // 2 M  =>  4 min  / 20  => 4 scond / 200 K => 31 sec   /
            // 200 k ~ 200 МБ  
            // 2000 K = 2 gb ON hdd - 4gb IN ram

            // Файл с которым идут операции
            string FILENAME = "data.txt";
            int K = 10; //кратность буфера для данных

            Console.WriteLine(" Запуск... строк {0}",SIZE);
            

            Stopwatch timer = Stopwatch.StartNew();

            //Очистка файла
            System.IO.StreamWriter out_file = System.IO.File.CreateText(FILENAME);
            out_file.Close();

            int i = 0;
            for (int z = 1; z <= K; z++)
            {
                List<StringData> data = new List<StringData>(Enumerable.Range(0, SIZE/K).Select(x => { i++; return new StringData(i); }));
                
                out_file = System.IO.File.AppendText(FILENAME);
                foreach (StringData ix in data)
                {
                    out_file.WriteLine(ix.Get);
                }
                out_file.Close();
            }
            timer.Stop();

            Console.WriteLine(" Кол-во строк в массиве {0}", SIZE);
            Console.WriteLine(" Создано за {0} сек ", timer.Elapsed);
            //Console.ReadKey();

            
            timer.Stop();
            


            //Console.ReadKey();
            Console.WriteLine(" Сортировка кусков...");
            timer = Stopwatch.StartNew();
            //--- частичная сортировка

            int CHUNK_COUNT_FILES = 10; //кол-во кусковых файлов
            int CHUNK_LEN = SIZE / CHUNK_COUNT_FILES + 1; //кол-во строк в файле кэша

            SortStringData sorter = new SortStringData(FILENAME, CHUNK_LEN);
            timer.Stop();
            Console.WriteLine(" Сортировка кусков {0}", timer.Elapsed);

            // --- Сведение файлов в 1 отсортированный массив

            Console.WriteLine(" Сборка кусков из частей и их сортировка в потоке ...");
            timer = Stopwatch.StartNew();
            SortViaCache processor = new SortViaCache(FILENAME, CHUNK_COUNT_FILES, SIZE);


            timer.Stop();
            Console.WriteLine(" Сортировка и склейка кусков {0}", timer.Elapsed);


            //Console.ReadKey();


        }
    }
}
