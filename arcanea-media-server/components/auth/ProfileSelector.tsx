import React, { useState } from 'react';
import { Plus, Pencil, X, Check, Trash2, Save } from 'lucide-react';
import { UserProfile } from '../../types';
import { useI18n } from '../../i18n/i18n';

interface ProfileSelectorProps {
  profiles: UserProfile[];
  onSelect: (profile: UserProfile) => void;
  onAdd: (profile: Omit<UserProfile, 'id'>) => void;
  onEdit: (profile: UserProfile) => void;
  onDelete: (id: string) => void;
}

const AVATAR_COLORS = [
  'bg-indigo-600', 'bg-purple-600', 'bg-pink-600', 'bg-red-600', 
  'bg-orange-600', 'bg-yellow-500', 'bg-green-600', 'bg-blue-600', 'bg-slate-600'
];

export const ProfileSelector: React.FC<ProfileSelectorProps> = ({ 
  profiles, 
  onSelect, 
  onAdd,
  onEdit,
  onDelete
}) => {
  const { t } = useI18n();
  const [isManaging, setIsManaging] = useState(false);
  const [editingProfile, setEditingProfile] = useState<Partial<UserProfile> | null>(null);
  const [avatarBusy, setAvatarBusy] = useState(false);

  const handleSave = (e: React.FormEvent) => {
    e.preventDefault();
    if (!editingProfile?.name) return;

    if (editingProfile.id) {
      // Update existing
      onEdit(editingProfile as UserProfile);
    } else {
      // Create new
      onAdd({
        name: editingProfile.name,
        avatarColor: editingProfile.avatarColor || AVATAR_COLORS[Math.floor(Math.random() * AVATAR_COLORS.length)],
        avatarImage: editingProfile.avatarImage,
        isKid: editingProfile.isKid || false,
        language: editingProfile.language || 'en-US'
      });
    }
    setEditingProfile(null);
  };

  const startAddProfile = () => {
    setEditingProfile({
      name: '',
      avatarColor: AVATAR_COLORS[Math.floor(Math.random() * AVATAR_COLORS.length)],
      isKid: false,
      language: 'en-US'
    });
  };

  return (
    <div className="fixed inset-0 z-50 bg-[#0b1220] flex flex-col items-center justify-center animate-fade-in">
      <h1 className="text-4xl md:text-5xl font-bold text-white mb-16 tracking-tight">
        {t('who_is_watching')}
      </h1>
      
      <div className="flex flex-wrap justify-center gap-8 md:gap-12 px-4 max-w-5xl">
        {profiles.map((profile) => (
          <div 
            key={profile.id}
            onClick={() => {
              onSelect(profile);
            }}
            className="group flex flex-col items-center cursor-pointer relative"
          >
            <div className={`
              w-32 h-32 md:w-40 md:h-40 rounded-xl mb-4 
              ${profile.avatarColor} 
              flex items-center justify-center shadow-2xl 
              border-2 border-transparent 
              group-hover:border-white
              transition-all duration-300 transform group-hover:scale-105
            `}>
              {profile.avatarImage ? (
                <img
                  src={profile.avatarImage}
                  alt={profile.name}
                  className="w-full h-full rounded-xl object-cover"
                  loading="lazy"
                />
              ) : profile.isKid ? (
                <span className="text-4xl">🎈</span>
              ) : (
                <span className="text-4xl font-bold text-white/90">{profile.name[0]}</span>
              )}

            </div>
            <span className="text-lg text-slate-400 group-hover:text-white transition-colors duration-300">
              {profile.name}
            </span>
          </div>
        ))}

        {/* Add Profile Button */}
        {false && <div 
          onClick={startAddProfile}
          className="group flex flex-col items-center cursor-pointer"
        >
          <div className="w-32 h-32 md:w-40 md:h-40 rounded-xl mb-4 bg-transparent border-2 border-slate-700 flex items-center justify-center hover:bg-slate-800 hover:border-slate-500 transition-all duration-300 transform hover:scale-105">
            <Plus size={48} className="text-slate-500 group-hover:text-slate-300" />
          </div>
          <span className="text-lg text-slate-500 group-hover:text-slate-400 transition-colors duration-300">
            {t('add_profile')}
          </span>
        </div>}
      </div>

      {false && <button 
        onClick={() => setIsManaging(!isManaging)}
        className={`
          mt-20 px-8 py-2 border text-sm font-semibold uppercase tracking-widest transition-all
          ${isManaging 
            ? 'bg-white text-black border-white hover:bg-slate-200' 
            : 'border-slate-600 text-slate-400 hover:text-white hover:border-white'
          }
        `}
      >
        {isManaging ? t('finish') : t('manage_profiles')}
      </button>}

      {/* Edit/Add Modal */}
      {false && editingProfile && (
        <div className="fixed inset-0 z-[60] bg-black/80 backdrop-blur-sm flex items-center justify-center p-4">
          <div className="bg-[#1e293b] w-full max-w-md p-8 rounded-2xl border border-white/10 shadow-2xl animate-slide-up">
            <h2 className="text-2xl font-bold text-white mb-6">
              {editingProfile.id ? t('edit_profile') : t('add_profile')}
            </h2>

            <form onSubmit={handleSave} className="space-y-6">
              <div className="flex justify-center mb-6">
                 <div className={`w-24 h-24 rounded-xl flex items-center justify-center shadow-lg ${editingProfile.avatarColor}`}>
                   {editingProfile.avatarImage ? (
                     <img
                       src={editingProfile.avatarImage}
                       alt={editingProfile.name || 'avatar'}
                       className="w-full h-full rounded-xl object-cover"
                     />
                   ) : (
                     <span className="text-4xl font-bold text-white">
                       {editingProfile.name ? editingProfile.name[0] : <User />}
                     </span>
                   )}
                 </div>
              </div>

              <div className="flex items-center justify-between gap-3">
                <label className="text-sm text-slate-300">
                  {t('photo_optional')}
                  <input
                    type="file"
                    accept="image/*"
                    disabled={avatarBusy}
                    onChange={async (e) => {
                      const f = e.target.files && e.target.files[0];
                      if (!f) return;
                      setAvatarBusy(true);
                      try {
                        const reader = new FileReader();
                        const dataUrl: string = await new Promise((resolve, reject) => {
                          reader.onerror = () => reject(new Error('read_failed'));
                          reader.onload = () => resolve(String(reader.result || ''));
                          reader.readAsDataURL(f);
                        });
                        setEditingProfile({ ...(editingProfile || {}), avatarImage: dataUrl });
                      } catch (err) {
                        // ignore
                      } finally {
                        setAvatarBusy(false);
                        try { e.target.value = ''; } catch { }
                      }
                    }}
                    className="block mt-2 text-xs text-slate-300 file:mr-3 file:py-2 file:px-3 file:rounded-md file:border-0 file:bg-slate-800 file:text-slate-200 hover:file:bg-slate-700"
                  />
                </label>
                {!!editingProfile.avatarImage && (
                  <button
                    type="button"
                    onClick={() => setEditingProfile({ ...(editingProfile || {}), avatarImage: undefined })}
                    className="px-3 py-2 text-xs text-slate-300 hover:text-white rounded-md border border-white/10 hover:bg-white/5 transition-colors"
                  >
                    {t('remove')}
                  </button>
                )}
              </div>

              <div>
                <input
                  type="text"
                  placeholder={t('profile_name')}
                  value={editingProfile.name}
                  onChange={e => setEditingProfile({...editingProfile, name: e.target.value})}
                  className="w-full bg-slate-900 border border-slate-700 rounded-lg px-4 py-3 text-white placeholder-slate-500 focus:outline-none focus:border-indigo-500 transition-colors"
                  autoFocus
                />
              </div>

              <div className="flex items-center space-x-3 p-3 bg-slate-900/50 rounded-lg border border-slate-700/50">
                 <input 
                   type="checkbox" 
                   id="modalIsKid"
                   checked={editingProfile.isKid}
                   onChange={e => setEditingProfile({...editingProfile, isKid: e.target.checked})}
                   className="w-5 h-5 rounded border-slate-600 bg-slate-800 text-indigo-500 focus:ring-indigo-500"
                 />
                 <label htmlFor="modalIsKid" className="flex-1 text-slate-300 cursor-pointer select-none">
                    {t('kid_profile')}
                 <span className="text-xs text-slate-500 block">{t('restricted_content')}</span>
                 </label>
              </div>

              <div className="flex justify-between items-center pt-4 border-t border-white/5">
                {editingProfile.id ? (
                  <button 
                    type="button"
                    onClick={() => {
                      if (profiles.length > 1) {
                         onDelete(editingProfile.id!);
                         setEditingProfile(null);
                      }
                    }}
                    className={`flex items-center px-4 py-2 text-red-400 hover:bg-red-500/10 rounded-lg transition-colors ${profiles.length <= 1 ? 'opacity-50 cursor-not-allowed' : ''}`}
                    disabled={profiles.length <= 1}
                  >
                    <Trash2 size={18} className="mr-2" /> {t('delete')}
                  </button>
                ) : (
                  <div></div> /* Spacer */
                )}

                <div className="flex space-x-3">
                  <button 
                    type="button"
                    onClick={() => setEditingProfile(null)}
                    className="px-6 py-2 text-slate-300 hover:text-white font-medium transition-colors"
                  >
                    {t('cancel')}
                  </button>
                  <button 
                    type="submit"
                    className="px-8 py-2 bg-indigo-600 hover:bg-indigo-500 text-white font-bold rounded-lg transition-colors shadow-lg shadow-indigo-500/25 flex items-center disabled:opacity-50 disabled:cursor-not-allowed"
                    disabled={!editingProfile.name}
                  >
                    <Save size={18} className="mr-2" /> {t('save')}
                  </button>
                </div>
              </div>
            </form>
          </div>
        </div>
      )}
    </div>
  );
};

function User(props: any) {
  return (
    <svg 
      {...props}
      xmlns="http://www.w3.org/2000/svg" 
      width="24" 
      height="24" 
      viewBox="0 0 24 24" 
      fill="none" 
      stroke="currentColor" 
      strokeWidth="2" 
      strokeLinecap="round" 
      strokeLinejoin="round"
    >
      <path d="M19 21v-2a4 4 0 0 0-4-4H9a4 4 0 0 0-4 4v2" />
      <circle cx="12" cy="7" r="4" />
    </svg>
  )
}
