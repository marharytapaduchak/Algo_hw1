#include <iostream>        // cout, cin
#include <string>          // std::string
#include <vector>          // std::vector
#include <unordered_map>   // std::unordered_map
#include <set>             // std::set
#include <queue>           // std::priority_queue
#include <algorithm>       // sort, partial_sort
#include <chrono>          // std::chrono::steady_clock
#include <random>          // std::mt19937, std::uniform_int_distribution
#include <fstream>
#include <sstream>
#include <filesystem> // C++17
#include <locale>
#include <codecvt>


using std::filesystem::current_path;
using std::filesystem::exists;
using std::filesystem::path;
using std::filesystem::absolute;


using namespace std;


struct Student {
    string m_name;
    string m_surname;
    string m_email;
    int m_birth_year;
    int m_birth_month;
    int m_birth_day;
    string m_group;
    float m_rating;
    string m_phone_number;
};

// ВАРІАНТ 1 — простий: vector + unordered_map + все рахуємо щоразу
class Student_V1 {
public:
    vector<Student> all;
    unordered_map<string, Student*> byEmail;

    Student_V1(const vector<Student>& data)
    {
        all = data;
        byEmail.reserve(all.size() * 2);
        for (Student& student : all)
        {
            byEmail[student.m_email] = &student;
        }
    }

    void top100(vector<Student>& out)
    {
        out.clear();
        if (all.size() <= 100)
        {
            out = all;
            sort(out.begin(), out.end(), [](const Student& a, const Student& b){
                return a.m_rating > b.m_rating;
            });
            return;
        }
        vector<Student> temp = all;
        sort(temp.begin(), temp.end(), [](const Student& a, const Student& b) {
            return a.m_rating > b.m_rating;
        });

        out.assign(temp.begin(), temp.begin() + 100);
    }

    bool set_rating_by_email(const string& email, float rating)
    {
        auto iter = byEmail.find(email);
        if (iter == byEmail.end()) return false;
        iter->second->m_rating = rating;
        return true;
    }

    string best_group_by_avg()
    {
        unordered_map<string, pair<double,int>> groups;
        for (Student& student : all)
        {
            pair<double, int>& temp = groups[student.m_group];
            temp.first += student.m_rating;
            temp.second += 1;
        }
        string res = "";
        double etalon = -1.0;
        for (auto& group : groups)
        {
            double average = group.second.first / group.second.second;
            if (average > etalon)
            {
                etalon = average;
                res = group.first;
            }
        }
        return res;
    }
};


// 3. ВАРІАНТ 2 — підтримуємо все: set by rating + стату по групах
class Student_V2 {
    struct RatingNode {
        Student* student;
        bool operator<(const RatingNode& other) const
        {
            if (student->m_rating != other.student->m_rating)
                return student->m_rating > other.student->m_rating;
            return student->m_email < other.student->m_email;
        }
    };

    struct GroupAverage {
        double sum = 0.0;
        int cnt = 0;

        double avg()
        {
            if (cnt != 0)
                return sum / cnt;
            return 0.0;
        }
    };

public:
    vector<Student> all;
    unordered_map<string, Student*> byEmail;
    set<RatingNode> byRating;
    unordered_map<string, GroupAverage> groups;

    Student_V2(const vector<Student>& data)
    {
        all = data;
        byEmail.reserve(all.size() * 2);
        for (Student& student : all)
        {
            byEmail[student.m_email] = &student;

            byRating.insert(RatingNode{&student});

            auto& group = groups[student.m_group];
            group.sum += student.m_rating;
            group.cnt += 1;
        }
    }

    void top100(vector<Student>& out)
    {
        out.clear();
        int cnt = 0;
        for (auto iter = byRating.begin(); iter != byRating.end() && cnt < 100; ++iter, ++cnt)
        {
            out.push_back(*(iter->student));
        }
    }

    bool set_rating_by_email(const string& email, float rating)
    {
        auto iter = byEmail.find(email);
        if (iter == byEmail.end()) return false;

        Student* student = iter->second;

        // видаляємо з рейтингів
        byRating.erase(RatingNode{student});

        // оновлюємо групу
        auto group_iter = groups.find(student->m_group);
        if (group_iter != groups.end())
        {
            group_iter->second.sum -= student->m_rating;
            group_iter->second.sum += rating;
        }

        student->m_rating = rating;
        byRating.insert(RatingNode{student});

        return true;
    }

    string best_group_by_avg()
    {
        string res = "";
        double etalon = -1.0;
        for (auto& group : groups)
        {
            double average = group.second.avg();
            if (average > etalon)
            {
                etalon = average;
                res = group.first;
            }
        }
        return res;
    }
};


// 4. ВАРІАНТ 3 — vector + email->index + heap(100) + групи
class Student_V3 {
    struct GroupAverage {
        double sum = 0.0;
        int cnt = 0;

        double avg()
        {
            if (cnt != 0)
                return sum / cnt;
            return 0.0;
        }
    };

    struct HeapEl {
        size_t idx;
        float rating;
    };

    struct MinHeapComparator {
        bool operator()(const HeapEl& a, const HeapEl& b) const
        {
            return a.rating > b.rating;
        }
    };

public:
    vector<Student> all;
    unordered_map<string, size_t> byEmail;
    unordered_map<string, GroupAverage> groups;
    priority_queue<HeapEl, vector<HeapEl>, MinHeapComparator> topHeap;

    Student_V3(const vector<Student>& data) {
        all = data;
        byEmail.reserve(all.size() * 2);
        for (size_t i = 0; i < all.size(); ++i)
        {
            byEmail[all[i].m_email] = i;

            GroupAverage& group = groups[all[i].m_group];
            group.sum += all[i].m_rating;
            group.cnt += 1;

            pushToTop(i);
        }
    }

    void pushToTop(size_t idx)
    {
        float rate = all[idx].m_rating;
        if (topHeap.size() < 100)
            topHeap.push(HeapEl{idx, rate});
        else
        {
            auto minEl = topHeap.top();
            if (rate > minEl.rating) {
                topHeap.pop();
                topHeap.push(HeapEl{idx, rate});
            }
        }
    }

    void top100(vector<Student>& out)
    {
        auto topHeap_ = topHeap;
        out.clear();
        while (!topHeap_.empty())
        {
            out.push_back(all[topHeap_.top().idx]);
            topHeap_.pop();
        }
        sort(out.begin(), out.end(), [](const Student& a, const Student& b){
            return a.m_rating > b.m_rating;
        });
    }

    bool set_rating_by_email(const string& email, float rating) {
        auto iter = byEmail.find(email);
        if (iter == byEmail.end()) return false;
        size_t idx = iter->second;
        Student& student = all[idx];

        GroupAverage& group = groups[student.m_group];
        group.sum -= student.m_rating;
        group.sum += rating;

        student.m_rating = rating;

        pushToTop(idx);
        return true;
    }

    string best_group_by_avg()
    {
        string res = "";
        double etalon = -1.0;
        for (auto& group : groups)
        {
            double average = group.second.avg();
            if (average > etalon)
            {
                etalon = average;
                res = group.first;
            }
        }
        return res;
    }
};




enum class OperationType { TOP100, SET_RATING, BEST_GROUP };

vector<OperationType> build_operations() {
    vector<OperationType> ops;
    ops.insert(ops.end(), 2, OperationType::TOP100);
    ops.insert(ops.end(), 10, OperationType::SET_RATING);
    ops.insert(ops.end(), 30, OperationType::BEST_GROUP);
    return ops;
}



template<typename T>
size_t run_example(T& data, const vector<string>& emails)
{
    int seconds = 10;
    auto operations = build_operations();
    mt19937 rng(123);
    uniform_int_distribution<int> opDist(0, (int)operations.size() - 1);
    uniform_int_distribution<int> emailDist(0, (int)emails.size() - 1);

    auto start = chrono::steady_clock::now();
    size_t done = 0;

    while (true) {
        auto now = chrono::steady_clock::now();
        auto elapsed = chrono::duration_cast<chrono::seconds>(now - start).count();
        if (elapsed >= seconds) break;

        OperationType op = operations[opDist(rng)];
        switch (op) {
            case OperationType::TOP100: {
                vector<Student> out;
                data.top100(out);
                break;
            }
            case OperationType::SET_RATING: {
                string em = emails[emailDist(rng)];
                float newRating = (float)(rng() % 101);
                data.set_rating_by_email(em, newRating);
                break;
            }
            case OperationType::BEST_GROUP: {
                data.best_group_by_avg();
                break;
            }
        }
        ++done;
    }
    return done;
}





vector<Student> read_csv(const string& filename) {
    vector<Student> students;
    ifstream file(filename);
    if (!file.is_open()) {
        cout << "Could not open file: " << filename << endl;
        return students;
    }

    string line;
    getline(file, line);

    while (getline(file, line)) {
        if (line.empty()) continue;

        stringstream stream_s(line);
        Student student;
        string temp;

        getline(stream_s, student.m_name, ',');
        getline(stream_s, student.m_surname, ',');
        getline(stream_s, student.m_email, ',');

        getline(stream_s, temp, ',');
        student.m_birth_year = stoi(temp);

        getline(stream_s, temp, ',');
        student.m_birth_month = stoi(temp);

        getline(stream_s, temp, ',');
        student.m_birth_day = stoi(temp);

        getline(stream_s, student.m_group, ',');

        getline(stream_s, temp, ',');
        student.m_rating = stof(temp);

        getline(stream_s, student.m_phone_number, ',');

        students.push_back(student);
    }

    cout << "Students uploaded: " << students.size() << endl;
    return students;
}

bool save_to_csv(const vector<Student>& data, const string& filename) {
    ofstream out(filename, ios::out | ios::trunc);
    if (!out.is_open()) {
        cout << "Failed to open for write: " << absolute(filename) << "\n";
        return false;
    }
    out << "m_name,m_surname,m_email,m_birth_year,m_birth_month,m_birth_day,m_group,m_rating,m_phone_number\n";
    for (const auto& s : data)
        out << s.m_name << ',' << s.m_surname << ',' << s.m_email << ','
            << s.m_birth_year << ',' << s.m_birth_month << ',' << s.m_birth_day << ','
            << s.m_group << ',' << s.m_rating << ',' << s.m_phone_number << '\n';
    out.flush();
    bool ok = (bool)out;
    if (!ok) cout << "Write failed (stream bad) for: " << absolute(filename) << "\n";
    return ok;
}




static locale uk_loc("uk_UA.UTF-8");

bool uk_less(const string& a, const string& b) {
    static wstring_convert<codecvt_utf8<wchar_t>> conv;
    wstring aw = conv.from_bytes(a);
    wstring bw = conv.from_bytes(b);
    const auto& coll = use_facet<collate<wchar_t>>(uk_loc);
    return coll.compare(aw.data(), aw.data()+aw.size(), bw.data(), bw.data()+bw.size()) < 0;
}

vector<Student> sort_with_std(vector<Student> data) {
    auto start = chrono::steady_clock::now();

    sort(data.begin(), data.end(), [](const Student& a, const Student& b) {
        if (a.m_name == b.m_name)
            return uk_less(a.m_surname, b.m_surname);
        return uk_less(a.m_name, b.m_name);
    });

    auto end = chrono::steady_clock::now();
    cout << "Std sort time: "
         << chrono::duration_cast<chrono::milliseconds>(end - start).count()
         << " ms" << endl;

    return  data;
}

vector<Student> sort_with_insertion(vector<Student> data)
{
    auto start = chrono::steady_clock::now();
    for (size_t i = 1; i < data.size(); ++i)
    {
        Student key = data[i];
        int j = (int)i - 1;

        while (j >= 0 && (uk_less(key.m_name, data[j].m_name) ||
            (key.m_name == data[j].m_name && uk_less(key.m_surname, data[j].m_surname))))
        {
            data[j + 1] = data[j];
            j--;
        }
        data[j + 1] = key;
    }
    auto end = chrono::steady_clock::now();
    cout << "Insertion sort time: "
         << chrono::duration_cast<chrono::milliseconds>(end - start).count()
         << " ms" << endl;

    return data;
}




#ifdef __APPLE__
#include <mach/mach.h>
double get_rss_mb() {
    task_basic_info_data_t info;
    mach_msg_type_number_t count = TASK_BASIC_INFO_COUNT;
    if (task_info(mach_task_self(), TASK_BASIC_INFO, (task_info_t)&info, &count) != KERN_SUCCESS)
        return -1.0;
    return info.resident_size / (1024.0 * 1024.0);
}
#else
double get_rss_mb() { return -1.0; }
#endif


struct BenchRow {
    string dataset;
    size_t n;
    string variant;
    size_t ops;
    double rss_mb;
};

void append_result_csv(const BenchRow& r, const string& path = "bench_ops.csv")
{
    ofstream out(path, ios::app);
    if (!out.is_open())
    {
        cout << "Cannot open " << path << " for append\n";
        return;
    }

    out << r.dataset << ','
        << r.n << ','
        << r.variant << ','
        << r.ops << ','
        << r.rss_mb << '\n';
}


int main() {
    vector<string> filenames = {"students-2_100.csv", "students-2_1000.csv", "students-2_10000.csv", "students-2.csv"};
    for (const string& filename : filenames)
    {
        vector<Student> data = read_csv(filename);
        if (data.empty()) {
            cout << "Error: file is empty or not read: " << filename << endl;
            continue;
        }

        vector<string> emails;
        emails.reserve(data.size());
        for (const Student& s : data) emails.push_back(s.m_email);

        //V1
        {
            Student_V1 v1(data);
            double rss_before = get_rss_mb(); (void)rss_before;
            size_t ops = run_example(v1, emails);
            double rss_after = get_rss_mb();
            append_result_csv({filename, data.size(), "V1", ops, rss_after});
            cout << "V1 " << filename << " ops=" << ops << " rss_mb=" << rss_after << "\n";
        }

        //V2
        {
            Student_V2 v2(data);
            double rss_before = get_rss_mb(); (void)rss_before;
            size_t ops = run_example(v2, emails);
            double rss_after = get_rss_mb();
            append_result_csv({filename, data.size(), "V2", ops, rss_after});
            cout << "V2 " << filename << " ops=" << ops << " rss_mb=" << rss_after << "\n";
        }

        //V3
        {
            Student_V3 v3(data);
            double rss_before = get_rss_mb(); (void)rss_before;
            size_t ops = run_example(v3, emails);
            double rss_after = get_rss_mb();
            append_result_csv({filename, data.size(), "V3", ops, rss_after});
            cout << "V3 " << filename << " ops=" << ops << " rss_mb=" << rss_after << "\n";
        }
    }


    vector<Student> data = read_csv("students-2_1000.csv");
    if (data.empty()) {
        cout << "No data loaded. Abort.\n";
        return 1;
    }

    // 1) std::sort
    vector<Student> data_1 = sort_with_std(data);
    // cout << "Writing: " << absolute("sort_1.csv") << "\n";
    save_to_csv(data_1, "sort_1.csv");

    // 2) insertion sort
    vector<Student> data_2 = sort_with_insertion(data);
    // cout << "Writing: " << absolute("sort_2.csv") << "\n";
    save_to_csv(data_2, "sort_2.csv");

    return 0;
}