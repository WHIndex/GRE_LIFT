#include"./src/src/core/lift.h"
#include"../indexInterface.h"

template<class KEY_TYPE, class PAYLOAD_TYPE>
class LIFTInterface : public indexInterface<KEY_TYPE, PAYLOAD_TYPE> {
public:
    void init(Param *param = nullptr) {}

    void bulk_load(std::pair <KEY_TYPE, PAYLOAD_TYPE> *key_value, size_t num, Param *param = nullptr);

    bool get(KEY_TYPE key, PAYLOAD_TYPE &val, Param *param = nullptr);

    bool put(KEY_TYPE key, PAYLOAD_TYPE value, Param *param = nullptr);

    bool update(KEY_TYPE key, PAYLOAD_TYPE value, Param *param = nullptr);

    bool remove(KEY_TYPE key, Param *param = nullptr);

    size_t scan(KEY_TYPE key_low_bound, size_t key_num, std::pair <KEY_TYPE, PAYLOAD_TYPE> *result,
                Param *param = nullptr);

    long long memory_consumption() { return lift.total_size(); }

private:
    lift::LIFT <KEY_TYPE, PAYLOAD_TYPE> lift;
};

template<class KEY_TYPE, class PAYLOAD_TYPE>
void LIFTInterface<KEY_TYPE, PAYLOAD_TYPE>::bulk_load(std::pair <KEY_TYPE, PAYLOAD_TYPE> *key_value, size_t num,
                                                      Param *param) {
    // std::cout << " sizeof(KEY_TYPE) " << sizeof(KEY_TYPE) <<  " sizeof(PAYLOAD_TYPE) " << sizeof(PAYLOAD_TYPE);
    lift.bulk_load(key_value, static_cast<int>(num));
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
bool LIFTInterface<KEY_TYPE, PAYLOAD_TYPE>::get(KEY_TYPE key, PAYLOAD_TYPE &val, Param *param) {
    bool exist;
    val = lift.at(key, false, exist);
    return exist;
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
bool LIFTInterface<KEY_TYPE, PAYLOAD_TYPE>::put(KEY_TYPE key, PAYLOAD_TYPE value, Param *param) {
    return lift.insert(key, value);

}

template<class KEY_TYPE, class PAYLOAD_TYPE>
bool LIFTInterface<KEY_TYPE, PAYLOAD_TYPE>::update(KEY_TYPE key, PAYLOAD_TYPE value, Param *param) {
    return lift.update(key, value);
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
bool LIFTInterface<KEY_TYPE, PAYLOAD_TYPE>::remove(KEY_TYPE key, Param *param) {
    return lift.remove(key);
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
size_t LIFTInterface<KEY_TYPE, PAYLOAD_TYPE>::scan(KEY_TYPE key_low_bound, size_t key_num,
                                                   std::pair <KEY_TYPE, PAYLOAD_TYPE> *result,
                                                   Param *param) {
    auto iter = lift.lower_bound(key_low_bound);
    int scan_size = 0;
    for (scan_size = 0; scan_size < key_num && !iter.is_end(); scan_size++) {
        result[scan_size] = {(*iter).first, (*iter).second};
        iter++;
    }
    return scan_size;    
}